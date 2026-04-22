/*
 * Copyright 2020 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer; // 声明包名：io.netty.buffer，这是 Netty 缓冲区相关的包。

import static io.netty.buffer.PoolThreadCache.*; // 静态导入 PoolThreadCache 中的常量，用于线程缓存相关的计算。

/**
 * SizeClasses requires {@code pageShifts} to be defined prior to inclusion,
 * 文档注释：解释 SizeClasses 类依赖 pageShifts 参数，并定义了一系列常量和表格结构。
 * 详细说明了 sizeClasses 表格的每个字段含义和计算公式。
 */
final class SizeClasses implements SizeClassesMetric {

    // 定义 final 类 SizeClasses，实现 SizeClassesMetric 接口，提供大小类度量方法。

    // 常量定义：LOG2_QUANTUM = 4，表示最小量子大小的对数，即 16 字节（1 << 4）。
    // 这是内存分配的最小单位，所有大小类都基于此对齐。
    static final int LOG2_QUANTUM = 4;

    // LOG2_SIZE_CLASS_GROUP = 2，表示每个大小类组的大小类数量的对数，即每个组有 4 个 (1 << 2) 大小类。
    // 大小类按组组织，每组大小翻倍。
    private static final int LOG2_SIZE_CLASS_GROUP = 2;

    // LOG2_MAX_LOOKUP_SIZE = 12，表示查找表最大大小的对数，即 4096 字节 (1 << 12)。
    // 用于快速查找小尺寸的索引。
    private static final int LOG2_MAX_LOOKUP_SIZE = 12;

    // sizeClasses 数组中每个元素的字段索引常量：
    private static final int LOG2GROUP_IDX = 1; // log2Group 字段索引：组基大小的对数。
    private static final int LOG2DELTA_IDX = 2; // log2Delta 字段索引：相邻大小类的 delta 对数。
    private static final int NDELTA_IDX = 3; // nDelta 字段索引：delta 乘数。
    private static final int PAGESIZE_IDX = 4; // isMultiPageSize 字段索引：是否为页大小的倍数 (0=no, 1=yes)。
    private static final int SUBPAGE_IDX = 5; // isSubPage 字段索引：是否为子页大小类 (0=no, 1=yes)。
    private static final int LOG2_DELTA_LOOKUP_IDX = 6; // log2DeltaLookup 字段索引：查找表用的 log2Delta (0=no)。

    // 标志常量：no=0, yes=1，用于布尔字段。
    private static final byte no = 0,
        yes = 1;

    // 实例字段：pageSize 是页面大小 (如 8192 字节)。
    final int pageSize;

    // pageShifts 是 pageSize 的对数 (如 13 表示 8192=1<<13)。
    final int pageShifts;

    // chunkSize 是块大小，通常是页面大小的倍数。
    final int chunkSize;

    // directMemoryCacheAlignment 是直接内存缓存对齐要求。
    final int directMemoryCacheAlignment;

    // nSizes：总大小类数量。
    final int nSizes;

    // nSubpages：子页大小类数量（小于页面大小的）。
    final int nSubpages;

    // nPSizes：页面大小倍数的大小类数量。
    final int nPSizes;

    // lookupMaxSize：查找表覆盖的最大尺寸。
    final int lookupMaxSize;

    // smallMaxSizeIdx：最大小尺寸类索引（子页最大索引）。
    final int smallMaxSizeIdx;

    // pageIdx2sizeTab：页面索引到大小的查找表，仅包含页面倍数大小。
    private final int[] pageIdx2sizeTab;

    // sizeIdx2sizeTab：大小类索引到实际大小的查找表，用于所有大小类。
    // lookup table for sizeIdx <= smallMaxSizeIdx
    private final int[] sizeIdx2sizeTab;

    // size2idxTab：尺寸到大小类索引的查找表，用于 size <= lookupMaxSize。
    // spacing is 1 << LOG2_QUANTUM, so the size of array is lookupMaxClass >> LOG2_QUANTUM
    private final int[] size2idxTab;

    // 构造函数：初始化大小类系统。
    SizeClasses(
        int pageSize,
        int pageShifts,
        int chunkSize,
        int directMemoryCacheAlignment
    ) {
        // 计算组数：chunkSize 的 log2 减去量子和组偏移。
        int group = log2(chunkSize) - LOG2_QUANTUM - LOG2_SIZE_CLASS_GROUP + 1;

        // 生成 sizeClasses 表格：group << LOG2_SIZE_CLASS_GROUP 个大小类，每个有 7 个 short 字段。
        //[index, log2Group, log2Delta, nDelta, isMultiPageSize, isSubPage, log2DeltaLookup]
        short[][] sizeClasses = new short[group << LOG2_SIZE_CLASS_GROUP][7];

        int normalMaxSize = -1; // 记录最后一个正常大小。
        int nSizes = 0; // 当前大小类计数。
        int size = 0; // 当前计算的大小。

        // 初始化第一个组：log2Group = LOG2_QUANTUM (4)，log2Delta = 4。
        int log2Group = LOG2_QUANTUM;
        int log2Delta = LOG2_QUANTUM;
        int ndeltaLimit = 1 << LOG2_SIZE_CLASS_GROUP; // 4

        // 第一小组，nDelta 从 0 开始。
        // first size class is 1 << LOG2_QUANTUM，即 16 字节。
        for (int nDelta = 0; nDelta < ndeltaLimit; nDelta++, nSizes++) {
            // 创建新大小类：调用 newSizeClass 计算所有字段。
            short[] sizeClass = newSizeClass(
                nSizes,
                log2Group,
                log2Delta,
                nDelta,
                pageShifts
            );
            sizeClasses[nSizes] = sizeClass; // 存入表格。
            size = sizeOf(sizeClass, directMemoryCacheAlignment); // 计算实际大小（考虑对齐）。
        }

        // 增加 log2Group 到下一个组基值。
        log2Group += LOG2_SIZE_CLASS_GROUP;

        // 剩余所有组，nDelta 从 1 开始，直到 size >= chunkSize。
        for (; size < chunkSize; log2Group++, log2Delta++) {
            for (
                int nDelta = 1;
                nDelta <= ndeltaLimit && size < chunkSize;
                nDelta++, nSizes++
            ) {
                short[] sizeClass = newSizeClass(
                    nSizes,
                    log2Group,
                    log2Delta,
                    nDelta,
                    pageShifts
                );
                sizeClasses[nSizes] = sizeClass;
                size = normalMaxSize = sizeOf(
                    sizeClass,
                    directMemoryCacheAlignment
                );
            }
        }

        // 断言：chunkSize 必须等于最后一个 normalMaxSize。
        assert chunkSize == normalMaxSize;

        // 遍历 sizeClasses 计算统计值：
        int smallMaxSizeIdx = 0; // 小尺寸最大索引。
        int lookupMaxSize = 0; // 查找表最大尺寸。
        int nPSizes = 0; // 页面倍数计数。
        int nSubpages = 0; // 子页计数。
        for (int idx = 0; idx < nSizes; idx++) {
            short[] sz = sizeClasses[idx];
            if (sz[PAGESIZE_IDX] == yes) {
                // 如果是页面倍数。
                nPSizes++;
            }
            if (sz[SUBPAGE_IDX] == yes) {
                // 如果是子页。
                nSubpages++;
                smallMaxSizeIdx = idx; // 更新小尺寸最大索引。
            }
            if (sz[LOG2_DELTA_LOOKUP_IDX] != no) {
                // 如果支持查找表。
                lookupMaxSize = sizeOf(sz, directMemoryCacheAlignment);
            }
        }
        // 赋值实例字段。
        this.smallMaxSizeIdx = smallMaxSizeIdx;
        this.lookupMaxSize = lookupMaxSize;
        this.nPSizes = nPSizes;
        this.nSubpages = nSubpages;
        this.nSizes = nSizes;

        // 赋值基本参数。
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        this.directMemoryCacheAlignment = directMemoryCacheAlignment;

        // 生成查找表：
        this.sizeIdx2sizeTab = newIdx2SizeTab(
            sizeClasses,
            nSizes,
            directMemoryCacheAlignment
        ); // 索引到大小。
        this.pageIdx2sizeTab = newPageIdx2sizeTab(
            sizeClasses,
            nSizes,
            nPSizes,
            directMemoryCacheAlignment
        ); // 页面索引到大小。
        this.size2idxTab = newSize2idxTab(lookupMaxSize, sizeClasses); // 尺寸到索引。
    }

    // 计算单个大小类所有字段。
    // calculate size class
    private static short[] newSizeClass(
        int index,
        int log2Group,
        int log2Delta,
        int nDelta,
        int pageShifts
    ) {
        short isMultiPageSize; // 是否页面倍数标志。
        if (log2Delta >= pageShifts) {
            // 如果 delta >= 页面大小，则一定是倍数。
            isMultiPageSize = yes;
        } else {
            int pageSize = 1 << pageShifts; // 计算页面大小。
            int size = calculateSize(log2Group, nDelta, log2Delta); // 计算 size = (1<<log2Group) + nDelta*(1<<log2Delta)。

            // 检查 size 是否为 pageSize 的倍数。
            isMultiPageSize = size == (size / pageSize) * pageSize ? yes : no;
        }

        // 计算 log2(nDelta)，如果 nDelta=0 则为 0。
        int log2Ndelta = nDelta == 0 ? 0 : log2(nDelta);

        // 检查是否需要移除某些位：1<<log2Ndelta < nDelta 时为 yes。
        byte remove = 1 << log2Ndelta < nDelta ? yes : no;

        // 计算 log2Size：通常为 log2Group，如果 log2Delta + log2Ndelta == log2Group 则 +1。
        int log2Size =
            log2Delta + log2Ndelta == log2Group ? log2Group + 1 : log2Group;
        if (log2Size == log2Group) {
            // 特殊情况设置 remove=yes。
            remove = yes;
        }

        // isSubpage：如果 log2Size < pageShifts + LOG2_SIZE_CLASS_GROUP，则为子页。
        short isSubpage =
            log2Size < pageShifts + LOG2_SIZE_CLASS_GROUP ? yes : no;

        // log2DeltaLookup：如果 log2Size <= LOG2_MAX_LOOKUP_SIZE 且无需 remove，则使用 log2Delta，否则 no。
        int log2DeltaLookup =
            log2Size < LOG2_MAX_LOOKUP_SIZE ||
            (log2Size == LOG2_MAX_LOOKUP_SIZE && remove == no)
                ? log2Delta
                : no;

        // 返回 7 个字段的数组。
        return new short[] {
            (short) index,
            (short) log2Group,
            (short) log2Delta, // index, log2Group, log2Delta
            (short) nDelta,
            isMultiPageSize,
            isSubpage,
            (short) log2DeltaLookup, // nDelta, isMultiPageSize, isSubpage, log2DeltaLookup
        };
    }

    // 生成 sizeIdx2sizeTab 查找表：遍历所有大小类，计算实际大小存入数组。
    private static int[] newIdx2SizeTab(
        short[][] sizeClasses,
        int nSizes,
        int directMemoryCacheAlignment
    ) {
        int[] sizeIdx2sizeTab = new int[nSizes]; // 创建 nSizes 大小的数组。

        for (int i = 0; i < nSizes; i++) {
            // 遍历每个大小类。
            short[] sizeClass = sizeClasses[i];
            sizeIdx2sizeTab[i] = sizeOf(sizeClass, directMemoryCacheAlignment); // 计算并存入实际大小。
        }
        return sizeIdx2sizeTab;
    }

    // 计算 size = (1 << log2Group) + (nDelta << log2Delta)。
    private static int calculateSize(int log2Group, int nDelta, int log2Delta) {
        return (1 << log2Group) + (nDelta << log2Delta);
    }

    // 计算 sizeClass 对应的实际大小，包括对齐调整。
    private static int sizeOf(
        short[] sizeClass,
        int directMemoryCacheAlignment
    ) {
        int log2Group = sizeClass[LOG2GROUP_IDX]; // 提取字段。
        int log2Delta = sizeClass[LOG2DELTA_IDX];
        int nDelta = sizeClass[NDELTA_IDX];

        int size = calculateSize(log2Group, nDelta, log2Delta); // 计算基础大小。

        // 如果需要对齐，则调整到 directMemoryCacheAlignment 的倍数。
        return alignSizeIfNeeded(size, directMemoryCacheAlignment);
    }

    // 生成 pageIdx2sizeTab：仅收集页面倍数大小类的大小。
    private static int[] newPageIdx2sizeTab(
        short[][] sizeClasses,
        int nSizes,
        int nPSizes,
        int directMemoryCacheAlignment
    ) {
        int[] pageIdx2sizeTab = new int[nPSizes]; // nPSizes 大小的数组。
        int pageIdx = 0; // 页面索引计数器。
        for (int i = 0; i < nSizes; i++) {
            short[] sizeClass = sizeClasses[i];
            if (sizeClass[PAGESIZE_IDX] == yes) {
                // 只收集页面倍数。
                pageIdx2sizeTab[pageIdx++] = sizeOf(
                    sizeClass,
                    directMemoryCacheAlignment
                );
            }
        }
        return pageIdx2sizeTab;
    }

    // 生成 size2idxTab：填充从 0 到 lookupMaxSize 的索引映射，每步 LOG2_QUANTUM。
    private static int[] newSize2idxTab(
        int lookupMaxSize,
        short[][] sizeClasses
    ) {
        int[] size2idxTab = new int[lookupMaxSize >> LOG2_QUANTUM]; // 数组大小：lookupMaxSize / 16。
        int idx = 0; // 数组索引。
        int size = 0; // 当前 size。

        for (int i = 0; size <= lookupMaxSize; i++) {
            // 遍历支持查找表的大小类。
            int log2Delta = sizeClasses[i][LOG2DELTA_IDX]; // 获取 log2Delta。
            int times = 1 << (log2Delta - LOG2_QUANTUM); // 计算该类覆盖的步数。

            // 填充 times 个条目，指向当前 sizeClass i。
            while (size <= lookupMaxSize && times-- > 0) {
                size2idxTab[idx++] = i; // 设置 size2idxTab[idx] = i。
                size = (idx + 1) << LOG2_QUANTUM; // size = (idx + 1) << 4，即下一个 16 字节边界。
            }
        }
        return size2idxTab;
    }

    // 接口方法：根据 sizeIdx 返回实际大小，使用查找表。
    @Override
    public int sizeIdx2size(int sizeIdx) {
        return sizeIdx2sizeTab[sizeIdx];
    }

    // 计算版本：不使用查找表，直接计算 sizeIdx 对应大小。
    @Override
    public int sizeIdx2sizeCompute(int sizeIdx) {
        int group = sizeIdx >> LOG2_SIZE_CLASS_GROUP; // 计算组号：sizeIdx / 4。
        int mod = sizeIdx & ((1 << LOG2_SIZE_CLASS_GROUP) - 1); // mod = sizeIdx % 4。

        // 第一组特殊处理：groupSize = 0。
        int groupSize =
            group == 0
                ? 0
                : (1 << (LOG2_QUANTUM + LOG2_SIZE_CLASS_GROUP - 1)) << group; // groupSize = 16 * 4^(group-1)。

        int shift = group == 0 ? 1 : group; // shift 计算。
        int lgDelta = shift + LOG2_QUANTUM - 1; // lgDelta = shift + 3。
        int modSize = (mod + 1) << lgDelta; // modSize = (mod + 1) << lgDelta。

        return groupSize + modSize; // 返回总大小。
    }

    // 接口方法：根据 pageIdx 返回页面倍数大小，使用查找表。
    @Override
    public long pageIdx2size(int pageIdx) {
        return pageIdx2sizeTab[pageIdx];
    }

    // 计算版本：类似 sizeIdx2sizeCompute，但基于页面大小。
    @Override
    public long pageIdx2sizeCompute(int pageIdx) {
        int group = pageIdx >> LOG2_SIZE_CLASS_GROUP;
        int mod = pageIdx & ((1 << LOG2_SIZE_CLASS_GROUP) - 1);

        // groupSize 基于 pageShifts。
        long groupSize =
            group == 0
                ? 0
                : (1L << (pageShifts + LOG2_SIZE_CLASS_GROUP - 1)) << group;

        int shift = group == 0 ? 1 : group;
        int log2Delta = shift + pageShifts - 1;
        int modSize = (mod + 1) << log2Delta;

        return groupSize + modSize;
    }

    // 接口方法：根据请求 size 返回对应大小类索引。
    @Override
    public int size2SizeIdx(int size) {
        if (size == 0) {
            // 特殊处理 size=0，返回 0。
            return 0;
        }
        if (size > chunkSize) {
            // 超过 chunkSize，返回 nSizes（表示使用 chunk）。
            return nSizes;
        }

        size = alignSizeIfNeeded(size, directMemoryCacheAlignment); // 对齐 size。

        if (size <= lookupMaxSize) {
            // 小尺寸使用查找表。
            // (size-1) >> 4 得到 size2idxTab 索引。
            return size2idxTab[(size - 1) >> LOG2_QUANTUM];
        }

        // 大尺寸计算：
        int x = log2((size << 1) - 1); // log2(2*size - 1)，近似 log2(size)。
        int shift =
            x <
            LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1 // 7
                ? 0
                : x - (LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM); // shift 计算。

        int group = shift << LOG2_SIZE_CLASS_GROUP; // group = shift * 4。

        int log2Delta =
            x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM
                : x - LOG2_SIZE_CLASS_GROUP - 1; // log2Delta 计算。

        int mod =
            ((size - 1) >> log2Delta) & ((1 << LOG2_SIZE_CLASS_GROUP) - 1); // mod = ((size-1) / (1<<log2Delta)) % 4。

        return group + mod; // 返回 sizeIdx。
    }

    // 接口方法：页面数到页面索引（向上取整）。
    @Override
    public int pages2pageIdx(int pages) {
        return pages2pageIdxCompute(pages, false);
    }

    // 接口方法：页面数到页面索引（向下取整）。
    @Override
    public int pages2pageIdxFloor(int pages) {
        return pages2pageIdxCompute(pages, true);
    }

    // 通用计算页面索引。
    private int pages2pageIdxCompute(int pages, boolean floor) {
        int pageSize = pages << pageShifts; // 总字节数。
        if (pageSize > chunkSize) {
            return nPSizes;
        }

        int x = log2((pageSize << 1) - 1); // 类似 size2SizeIdx。

        int shift =
            x <
            LOG2_SIZE_CLASS_GROUP + pageShifts // pageShifts 代替 LOG2_QUANTUM
                ? 0
                : x - (LOG2_SIZE_CLASS_GROUP + pageShifts);

        int group = shift << LOG2_SIZE_CLASS_GROUP;

        int log2Delta =
            x < LOG2_SIZE_CLASS_GROUP + pageShifts + 1
                ? pageShifts
                : x - LOG2_SIZE_CLASS_GROUP - 1;

        int mod =
            ((pageSize - 1) >> log2Delta) & ((1 << LOG2_SIZE_CLASS_GROUP) - 1);

        int pageIdx = group + mod;

        // 如果 floor 且计算的大小 > 请求，则减 1。
        if (floor && pageIdx2sizeTab[pageIdx] > pages << pageShifts) {
            pageIdx--;
        }

        return pageIdx;
    }

    // 将 size 向上对齐到 directMemoryCacheAlignment 的倍数。
    // Round size up to the nearest multiple of alignment.
    private static int alignSizeIfNeeded(
        int size,
        int directMemoryCacheAlignment
    ) {
        if (directMemoryCacheAlignment <= 0) {
            // 无需对齐直接返回。
            return size;
        }
        int delta = size & (directMemoryCacheAlignment - 1); // 低位掩码结果。
        return delta == 0 ? size : size + directMemoryCacheAlignment - delta; // 如果已对齐返回，否则加差值。
    }

    // 接口方法：标准化 size 到最近的大小类大小。
    @Override
    public int normalizeSize(int size) {
        if (size == 0) {
            return sizeIdx2sizeTab[0]; // size=0 返回最小大小。
        }
        size = alignSizeIfNeeded(size, directMemoryCacheAlignment); // 对齐。
        if (size <= lookupMaxSize) {
            // 使用查找表。
            int ret = sizeIdx2sizeTab[size2idxTab[(size - 1) >> LOG2_QUANTUM]];
            assert ret == normalizeSizeCompute(size); // 验证计算一致。
            return ret;
        }
        return normalizeSizeCompute(size); // 大尺寸计算。
    }

    // 计算标准化 size：向上取到下一个 delta 边界，其中 delta 根据 size 的 log2 计算。
    private static int normalizeSizeCompute(int size) {
        int x = log2((size << 1) - 1); // log2(2*size -1)。
        int log2Delta =
            x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM
                : x - LOG2_SIZE_CLASS_GROUP - 1; // 计算 delta log2。
        int delta = 1 << log2Delta; // delta = 1 << log2Delta。
        int delta_mask = delta - 1; // 低位掩码。
        // size + delta_mask & ~delta_mask：将高位对齐，相当于向上取到 delta 倍数。
        return (size + delta_mask) & ~delta_mask;
    }
}
