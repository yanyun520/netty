/*
 * Copyright 2022 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.IntSupplier;
import io.netty.util.NettyRuntime;
import io.netty.util.Recycler.EnhancedHandle;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.MpscAtomicIntegerArrayQueue;
import io.netty.util.concurrent.MpscIntQueue;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReferenceCountUpdater;
import io.netty.util.internal.SuppressJava6Requirement;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.ThreadLocalRandom;
import io.netty.util.internal.UnstableApi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.StampedLock;

/**
 * An auto-tuning pooling allocator, that follows an anti-generational hypothesis.
 * <p>
 * The allocator is organized into a list of Magazines, and each magazine has a chunk-buffer that they allocate buffers
 * from.
 * <p>
 * The magazines hold the mutexes that ensure the thread-safety of the allocator, and each thread picks a magazine
 * based on the id of the thread. This spreads the contention of multi-threaded access across the magazines.
 * If contention is detected above a certain threshold, the number of magazines are increased in response to the
 * contention.
 * <p>
 * The magazines maintain histograms of the sizes of the allocations they do. The histograms are used to compute the
 * preferred chunk size. The preferred chunk size is one that is big enough to service 10 allocations of the
 * 99-percentile size. This way, the chunk size is adapted to the allocation patterns.
 * <p>
 * Computing the preferred chunk size is a somewhat expensive operation. Therefore, the frequency with which this is
 * done, is also adapted to the allocation pattern. If a newly computed preferred chunk is the same as the previous
 * preferred chunk size, then the frequency is reduced. Otherwise, the frequency is increased.
 * <p>
 * This allows the allocator to quickly respond to changes in the application workload,
 * without suffering undue overhead from maintaining its statistics.
 * <p>
 * Since magazines are "relatively thread-local", the allocator has a central queue that allow excess chunks from any
 * magazine, to be shared with other magazines.
 * The {@link #createSharedChunkQueue()} method can be overridden to customize this queue.
 */
@SuppressJava6Requirement(reason = "Guarded by version check")
@UnstableApi
final class AdaptivePoolingAllocator implements AdaptiveByteBufAllocator.AdaptiveAllocatorApi {
    /**
     * The 128 KiB minimum chunk size is chosen to encourage the system allocator to delegate to mmap for chunk
     * allocations. For instance, glibc will do this.
     * This pushes any fragmentation from chunk size deviations off physical memory, onto virtual memory,
     * which is a much, much larger space. Chunks are also allocated in whole multiples of the minimum
     * chunk size, which itself is a whole multiple of popular page sizes like 4 KiB, 16 KiB, and 64 KiB.
     */
    private static final int MIN_CHUNK_SIZE = 128 * 1024;
    private static final int EXPANSION_ATTEMPTS = 3;
    private static final int INITIAL_MAGAZINES = 1;
    private static final int RETIRE_CAPACITY = 256;
    private static final int MAX_STRIPES = NettyRuntime.availableProcessors() * 2;
    private static final int BUFS_PER_CHUNK = 8; // For large buffers, aim to have about this many buffers per chunk.

    /**
     * The maximum size of a pooled chunk, in bytes. Allocations bigger than this will never be pooled.
     * <p>
     * This number is 8 MiB, and is derived from the limitations of internal histograms.
     */
    private static final int MAX_CHUNK_SIZE = 8 * 1024 * 1024; // 8 MiB.
    private static final int MAX_POOLED_BUF_SIZE = MAX_CHUNK_SIZE / BUFS_PER_CHUNK;

    /**
     * The capacity if the chunk reuse queues, that allow chunks to be shared across magazines in a group.
     * The default size is twice {@link NettyRuntime#availableProcessors()},
     * same as the maximum number of magazines per magazine group.
     */
    private static final int CHUNK_REUSE_QUEUE = Math.max(2, SystemPropertyUtil.getInt(
            "io.netty.allocator.chunkReuseQueueCapacity", NettyRuntime.availableProcessors() * 2));

    /**
     * The capacity if the magazine local buffer queue. This queue just pools the outer ByteBuf instance and not
     * the actual memory and so helps to reduce GC pressure.
     */
    private static final int MAGAZINE_BUFFER_QUEUE_CAPACITY = SystemPropertyUtil.getInt(
            "io.netty.allocator.magazineBufferQueueCapacity", 1024);

    /**
     * The size classes are chosen based on the following observation:
     * <p>
     * Most allocations, particularly ones above 256 bytes, aim to be a power-of-2. However, many use cases, such
     * as framing protocols, are themselves operating or moving power-of-2 sized payloads, to which they add a
     * small amount of overhead, such as headers or checksums.
     * This means we seem to get a lot of mileage out of having both power-of-2 sizes, and power-of-2-plus-a-bit.
     * <p>
     * On the conflicting requirements of both having as few chunks as possible, and having as little wasted
     * memory within each chunk as possible, this seems to strike a surprisingly good balance for the use cases
     * tested so far.
     */
    private static final int[] SIZE_CLASSES = {
            32,
            64,
            128,
            256,
            512,
            640, // 512 + 128
            1024,
            1152, // 1024 + 128
            2048,
            2304, // 2048 + 256
            4096,
            4352, // 4096 + 256
            8192,
            8704, // 8192 + 512
            16384,
            16896, // 16384 + 512
    };
    private static final ChunkReleasePredicate CHUNK_RELEASE_ALWAYS = new ChunkReleasePredicate() {
        @Override
        public boolean shouldReleaseChunk(int chunkSize) {
            return true;
        }
    };
    private static final ChunkReleasePredicate CHUNK_RELEASE_NEVER = new ChunkReleasePredicate() {
        @Override
        public boolean shouldReleaseChunk(int chunkSize) {
            return false;
        }
    };

    private static final int SIZE_CLASSES_COUNT = SIZE_CLASSES.length;
    private static final byte[] SIZE_INDEXES = new byte[(SIZE_CLASSES[SIZE_CLASSES_COUNT - 1] / 32) + 1];

    static {
        if (MAGAZINE_BUFFER_QUEUE_CAPACITY < 2) {
            throw new IllegalArgumentException("MAGAZINE_BUFFER_QUEUE_CAPACITY: " + MAGAZINE_BUFFER_QUEUE_CAPACITY
                    + " (expected: >= " + 2 + ')');
        }
        int lastIndex = 0;
        for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
            int sizeClass = SIZE_CLASSES[i];
            //noinspection ConstantValue
            assert (sizeClass & 5) == 0 : "Size class must be a multiple of 32";
            int sizeIndex = sizeIndexOf(sizeClass);
            Arrays.fill(SIZE_INDEXES, lastIndex + 1, sizeIndex + 1, (byte) i);
            lastIndex = sizeIndex;
        }
    }

    private final ChunkAllocator chunkAllocator;
    private final Set<Chunk> chunkRegistry;
    private final MagazineGroup[] sizeClassedMagazineGroups;
    private final MagazineGroup largeBufferMagazineGroup;
    private final FastThreadLocal<MagazineGroup[]> threadLocalGroup;

    AdaptivePoolingAllocator(ChunkAllocator chunkAllocator, final boolean useCacheForNonEventLoopThreads) {
        this.chunkAllocator = ObjectUtil.checkNotNull(chunkAllocator, "chunkAllocator");
        chunkRegistry = Collections.<Chunk>newSetFromMap(PlatformDependent.<Chunk, Boolean>newConcurrentHashMap());
        sizeClassedMagazineGroups = createMagazineGroupSizeClasses(this, false);
        largeBufferMagazineGroup = new MagazineGroup(
                this, chunkAllocator, new HistogramChunkControllerFactory(true), false);
        threadLocalGroup = new FastThreadLocal<MagazineGroup[]>() {
            @Override
            protected MagazineGroup[] initialValue() {
                if (useCacheForNonEventLoopThreads || ThreadExecutorMap.currentExecutor() != null) {
                    return createMagazineGroupSizeClasses(AdaptivePoolingAllocator.this, true);
                }
                return null;
            }

            @Override
            protected void onRemoval(final MagazineGroup[] groups) throws Exception {
                if (groups != null) {
                    for (MagazineGroup group : groups) {
                        group.free();
                    }
                }
            }
        };
    }

    private static MagazineGroup[] createMagazineGroupSizeClasses(
            AdaptivePoolingAllocator allocator, boolean isThreadLocal) {
        MagazineGroup[] groups = new MagazineGroup[SIZE_CLASSES.length];
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            int segmentSize = SIZE_CLASSES[i];
            groups[i] = new MagazineGroup(allocator, allocator.chunkAllocator,
                    new SizeClassChunkControllerFactory(segmentSize), isThreadLocal);
        }
        return groups;
    }

    /**
     * Create a thread-safe multi-producer, multi-consumer queue to hold chunks that spill over from the
     * internal Magazines.
     * <p>
     * Each Magazine can only hold two chunks at any one time: the chunk it currently allocates from,
     * and the next-in-line chunk which will be used for allocation once the current one has been used up.
     * This queue will be used by magazines to share any excess chunks they allocate, so that they don't need to
     * allocate new chunks when their current and next-in-line chunks have both been used up.
     * <p>
     * The simplest implementation of this method is to return a new {@link ConcurrentLinkedQueue}.
     * However, the {@code CLQ} is unbounded, and this means there's no limit to how many chunks can be cached in this
     * queue.
     * <p>
     * Each chunk in this queue can be up to {@link #MAX_CHUNK_SIZE} in size, so it is recommended to use a bounded
     * queue to limit the maximum memory usage.
     * <p>
     * The default implementation will create a bounded queue with a capacity of {@link #CHUNK_REUSE_QUEUE}.
     *
     * @return A new multi-producer, multi-consumer queue.
     */
    private static Queue<Chunk> createSharedChunkQueue() {
        return PlatformDependent.newFixedMpmcQueue(CHUNK_REUSE_QUEUE);
    }

    @Override
    public ByteBuf allocate(int size, int maxCapacity) {
        return allocate(size, maxCapacity, Thread.currentThread(), null);
    }

    // ========== 核心分配方法：allocate ==========
    private AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
        // ========== 步骤 1: 初始化已分配标志 ==========
        // allocated 用于记录是否成功分配了内存
        // 初始值为 null，表示还未尝试分配
        AdaptiveByteBuf allocated = null;
        
        // ========== 步骤 2: 检查申请的大小是否在池化范围内 ==========
        // MAX_POOLED_BUF_SIZE = MAX_CHUNK_SIZE / BUFS_PER_CHUNK = 8MB / 8 = 1MB
        // 如果申请的大小超过 1MB，则无法使用池化分配器，后续会使用 allocateFallback
        if (size <= MAX_POOLED_BUF_SIZE) {
            // ========== 步骤 2a: 根据大小计算对应的 size class 索引 ==========
            // SIZE_CLASSES 中有 16 个预定义的大小类别（32 字节到 16KB）
            // sizeClassIndexOf(size) 会根据请求的大小返回对应的索引
            // 例如：申请 100 字节会被映射到 128 字节的 size class
            final int index = sizeClassIndexOf(size);
            
            // ========== 步骤 2b: 获取线程本地的 MagazineGroup 数组 ==========
            MagazineGroup[] magazineGroups;
            
            // 双重条件判断：
            // 1. !FastThreadLocalThread.willCleanupFastThreadLocals(currentThread)
            //    - 如果当前线程不是 FastThreadLocalThread，无法自动清理 FastThreadLocal
            //    - 此时应该使用全局的 sizeClassedMagazineGroups（非线程本地）
            // 2. || (magazineGroups = threadLocalGroup.get()) == null
            //    - 或者尝试获取线程本地的 MagazineGroup[] 失败（返回 null）
            //    - 这种情况发生在：线程已清理、useCacheForNonEventLoopThreads=false 等场景
            if (!FastThreadLocalThread.willCleanupFastThreadLocals(currentThread) ||
                    (magazineGroups = threadLocalGroup.get()) == null) {
                // 使用全局的 MagazineGroup[]（在所有线程间共享，但通过 magazine 的线程亲和性分散竞争）
                magazineGroups = sizeClassedMagazineGroups;
            }
            
            // ========== 步骤 2c: 根据索引选择合适的 MagazineGroup ==========
            if (index < magazineGroups.length) {
                // index 在有效范围内（0-15，对应 16 个 size class）
                // 从对应的 MagazineGroup 中分配
                allocated = magazineGroups[index].allocate(size, maxCapacity, currentThread, buf);
            } else {
                // index >= 16，说明大小不属于任何 size class
                // 使用 largeBufferMagazineGroup 处理大型缓冲区（1MB-8MB）
                // largeBufferMagazineGroup 使用直方图统计，自动调整 chunk 大小
                allocated = largeBufferMagazineGroup.allocate(size, maxCapacity, currentThread, buf);
            }
        }
        
        // ========== 步骤 3: 如果池化分配失败，使用回退方案 ==========
        // allocated == null 的原因：
        // - size > MAX_POOLED_BUF_SIZE（超大分配）
        // - 或者所有 magazine 都处于高竞争状态，无法获取锁
        if (allocated == null) {
            // allocateFallback 会为这个特定的分配创建一个临时 chunk
            // 该 chunk 不会被池化，分配完成后会被释放
            allocated = allocateFallback(size, maxCapacity, currentThread, buf);
        }
        
        // ========== 步骤 4: 返回分配结果 ==========
        // 返回值可能为 null（在极端情况下，比如 allocateFallback 也失败）
        return allocated;
    }

    private static int sizeIndexOf(final int size) {
        // this is aligning the size to the next multiple of 32 and dividing by 32 to get the size index.
        return size + 31 >> 5;
    }

    static int sizeClassIndexOf(int size) {
        int sizeIndex = sizeIndexOf(size);
        if (sizeIndex < SIZE_INDEXES.length) {
            return SIZE_INDEXES[sizeIndex];
        }
        return SIZE_CLASSES_COUNT;
    }

    static int[] getSizeClasses() {
        return SIZE_CLASSES.clone();
    }

    private AdaptiveByteBuf allocateFallback(int size, int maxCapacity, Thread currentThread,
                                             AdaptiveByteBuf buf) {
        // If we don't already have a buffer, obtain one from the most conveniently available magazine.
        Magazine magazine;
        if (buf != null) {
            Chunk chunk = buf.chunk;
            if (chunk == null || chunk == Magazine.MAGAZINE_FREED || (magazine = chunk.currentMagazine()) == null) {
                magazine = getFallbackMagazine(currentThread);
            }
        } else {
            magazine = getFallbackMagazine(currentThread);
            buf = magazine.newBuffer();
        }
        // Create a one-off chunk for this allocation.
        AbstractByteBuf innerChunk = chunkAllocator.allocate(size, maxCapacity);
        Chunk chunk = new Chunk(innerChunk, magazine, false, CHUNK_RELEASE_ALWAYS);
        try {
            chunk.readInitInto(buf, size, size, maxCapacity);
        } finally {
            // As the chunk is an one-off we need to always call release explicitly as readInitInto(...)
            // will take care of retain once when successful. Once The AdaptiveByteBuf is released it will
            // completely release the Chunk and so the contained innerChunk.
            chunk.release();
        }
        return buf;
    }

    private Magazine getFallbackMagazine(Thread currentThread) {
        Magazine[] mags = largeBufferMagazineGroup.magazines;
        return mags[(int) currentThread.getId() & mags.length - 1];
    }

    /**
     * Allocate into the given buffer. Used by {@link AdaptiveByteBuf#capacity(int)}.
     */
    void reallocate(int size, int maxCapacity, AdaptiveByteBuf into) {
        AdaptiveByteBuf result = allocate(size, maxCapacity, Thread.currentThread(), into);
        assert result == into: "Re-allocation created separate buffer instance";
    }

    @Override
    public long usedMemory() {
        long sum = 0;
        for (Chunk chunk : chunkRegistry) {
            sum += chunk.capacity();
        }
        return sum;
    }

    // Ensure that we release all previous pooled resources when this object is finalized. This is needed as otherwise
    // we might end up with leaks. While these leaks are usually harmless in reality it would still at least be
    // very confusing for users.
    @SuppressWarnings({"FinalizeDeclaration", "deprecation"})
    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            free();
        }
    }

    private void free() {
        largeBufferMagazineGroup.free();
    }

    static int sizeToBucket(int size) {
        return HistogramChunkController.sizeToBucket(size);
    }

    @SuppressJava6Requirement(reason = "Guarded by version check")
    private static final class MagazineGroup {
        private final AdaptivePoolingAllocator allocator;
        private final ChunkAllocator chunkAllocator;
        private final ChunkControllerFactory chunkControllerFactory;
        private final Queue<Chunk> chunkReuseQueue;
        private final StampedLock magazineExpandLock;
        private final Magazine threadLocalMagazine;
        private volatile Magazine[] magazines;
        private volatile boolean freed;

        MagazineGroup(AdaptivePoolingAllocator allocator,
                      ChunkAllocator chunkAllocator,
                      ChunkControllerFactory chunkControllerFactory,
                      boolean isThreadLocal) {
            this.allocator = allocator;
            this.chunkAllocator = chunkAllocator;
            this.chunkControllerFactory = chunkControllerFactory;
            chunkReuseQueue = createSharedChunkQueue();
            if (isThreadLocal) {
                magazineExpandLock = null;
                threadLocalMagazine = new Magazine(this, false, chunkReuseQueue, chunkControllerFactory.create(this));
            } else {
                magazineExpandLock = new StampedLock();
                threadLocalMagazine = null;
                Magazine[] mags = new Magazine[INITIAL_MAGAZINES];
                for (int i = 0; i < mags.length; i++) {
                    mags[i] = new Magazine(this, true, chunkReuseQueue, chunkControllerFactory.create(this));
                }
                magazines = mags;
            }
        }

        public AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
            boolean reallocate = buf != null;

            // Path for thread-local allocation.
            Magazine tlMag = threadLocalMagazine;
            if (tlMag != null) {
                if (buf == null) {
                    buf = tlMag.newBuffer();
                }
                boolean allocated = tlMag.tryAllocate(size, maxCapacity, buf, reallocate);
                assert allocated : "Allocation of threadLocalMagazine must always succeed";
                return buf;
            }

            // Path for concurrent allocation.
            long threadId = currentThread.getId();
            Magazine[] mags;
            int expansions = 0;
            do {
                mags = magazines;
                int mask = mags.length - 1;
                int index = (int) (threadId & mask);
                for (int i = 0, m = mags.length << 1; i < m; i++) {
                    Magazine mag = mags[index + i & mask];
                    if (buf == null) {
                        buf = mag.newBuffer();
                    }
                    if (mag.tryAllocate(size, maxCapacity, buf, reallocate)) {
                        // Was able to allocate.
                        return buf;
                    }
                }
                expansions++;
            } while (expansions <= EXPANSION_ATTEMPTS && tryExpandMagazines(mags.length));

            // The magazines failed us; contention too high and we don't want to spend more effort expanding the array.
            if (!reallocate && buf != null) {
                buf.release(); // Release the previously claimed buffer before we return.
            }
            return null;
        }

        private boolean tryExpandMagazines(int currentLength) {
            if (currentLength >= MAX_STRIPES) {
                return true;
            }
            final Magazine[] mags;
            long writeLock = magazineExpandLock.tryWriteLock();
            if (writeLock != 0) {
                try {
                    mags = magazines;
                    if (mags.length >= MAX_STRIPES || mags.length > currentLength || freed) {
                        return true;
                    }
                    Magazine firstMagazine = mags[0];
                    Magazine[] expanded = new Magazine[mags.length * 2];
                    for (int i = 0, l = expanded.length; i < l; i++) {
                        Magazine m = new Magazine(this, true, chunkReuseQueue, chunkControllerFactory.create(this));
                        firstMagazine.initializeSharedStateIn(m);
                        expanded[i] = m;
                    }
                    magazines = expanded;
                } finally {
                    magazineExpandLock.unlockWrite(writeLock);
                }
                for (Magazine magazine : mags) {
                    magazine.free();
                }
            }
            return true;
        }

        boolean offerToQueue(Chunk buffer) {
            if (freed) {
                return false;
            }

            boolean isAdded = chunkReuseQueue.offer(buffer);
            if (freed && isAdded) {
                // Help to free the reuse queue.
                freeChunkReuseQueue();
            }
            return isAdded;
        }

        private void free() {
            freed = true;
            if (threadLocalMagazine != null) {
                threadLocalMagazine.free();
            } else {
                long stamp = magazineExpandLock.writeLock();
                try {
                    Magazine[] mags = magazines;
                    for (Magazine magazine : mags) {
                        magazine.free();
                    }
                } finally {
                    magazineExpandLock.unlockWrite(stamp);
                }
            }
            freeChunkReuseQueue();
        }

        private void freeChunkReuseQueue() {
            for (;;) {
                Chunk chunk = chunkReuseQueue.poll();
                if (chunk == null) {
                    break;
                }
                chunk.release();
            }
        }
    }

    private interface ChunkControllerFactory {
        ChunkController create(MagazineGroup group);
    }

    private interface ChunkController {
        /**
         * Compute the "fast max capacity" value for the buffer.
         */
        int computeBufferCapacity(int requestedSize, int maxCapacity, boolean isReallocation);

        /**
         * Initialize the given chunk factory with shared statistics state (if any) from this factory.
         */
        void initializeSharedStateIn(ChunkController chunkController);

        /**
         * Allocate a new {@link Chunk} for the given {@link Magazine}.
         */
        Chunk newChunkAllocation(int promptingSize, Magazine magazine);
    }

    private interface ChunkReleasePredicate {
        boolean shouldReleaseChunk(int chunkSize);
    }

    private static final class SizeClassChunkControllerFactory implements ChunkControllerFactory {
        private final int segmentSize;

        private SizeClassChunkControllerFactory(int segmentSize) {
            this.segmentSize = ObjectUtil.checkPositive(segmentSize, "segmentSize");
        }

        @Override
        public ChunkController create(MagazineGroup group) {
            return new SizeClassChunkController(group, segmentSize);
        }
    }

    private static final class SizeClassChunkController implements ChunkController {
        // To amortize activation/deactivation of chunks, we should have a minimum number of segments per chunk.
        // We choose 32 because it seems neither too small nor too big.
        // For segments of 16 KiB, the chunks will be half a megabyte.
        private static final int MIN_SEGMENTS_PER_CHUNK = 32;
        private final ChunkAllocator chunkAllocator;
        private final int segmentSize;
        private final int chunkSize;
        private final Set<Chunk> chunkRegistry;

        private SizeClassChunkController(MagazineGroup group, int segmentSize) {
            chunkAllocator = group.chunkAllocator;
            this.segmentSize = segmentSize;
            chunkSize = Math.max(MIN_CHUNK_SIZE, segmentSize * MIN_SEGMENTS_PER_CHUNK);
            chunkRegistry = group.allocator.chunkRegistry;
        }

        @Override
        public int computeBufferCapacity(
                int requestedSize, int maxCapacity, boolean isReallocation) {
            return Math.min(segmentSize, maxCapacity);
        }

        @Override
        public void initializeSharedStateIn(ChunkController chunkController) {
            // NOOP
        }

        @Override
        public Chunk newChunkAllocation(int promptingSize, Magazine magazine) {
            SizeClassedChunk chunk = new SizeClassedChunk(chunkAllocator.allocate(chunkSize, chunkSize),
                    magazine, true, segmentSize, CHUNK_RELEASE_NEVER);
            chunkRegistry.add(chunk);
            return chunk;
        }
    }

    private static final class HistogramChunkControllerFactory implements ChunkControllerFactory {
        private final boolean shareable;

        private HistogramChunkControllerFactory(boolean shareable) {
            this.shareable = shareable;
        }

        @Override
        public ChunkController create(MagazineGroup group) {
            return new HistogramChunkController(group, shareable);
        }
    }

    private static final class HistogramChunkController implements ChunkController, ChunkReleasePredicate {
        private static final int MIN_DATUM_TARGET = 1024;
        private static final int MAX_DATUM_TARGET = 65534;
        private static final int INIT_DATUM_TARGET = 9;
        private static final int HISTO_BUCKET_COUNT = 16;
        private static final int[] HISTO_BUCKETS = {
                16 * 1024,
                24 * 1024,
                32 * 1024,
                48 * 1024,
                64 * 1024,
                96 * 1024,
                128 * 1024,
                192 * 1024,
                256 * 1024,
                384 * 1024,
                512 * 1024,
                768 * 1024,
                1024 * 1024,
                1792 * 1024,
                2048 * 1024,
                3072 * 1024
        };

        private final MagazineGroup group;
        private final boolean shareable;
        private final short[][] histos = {
                new short[HISTO_BUCKET_COUNT], new short[HISTO_BUCKET_COUNT],
                new short[HISTO_BUCKET_COUNT], new short[HISTO_BUCKET_COUNT],
        };
        private final Set<Chunk> chunkRegistry;
        private short[] histo = histos[0];
        private final int[] sums = new int[HISTO_BUCKET_COUNT];

        private int histoIndex;
        private int datumCount;
        private int datumTarget = INIT_DATUM_TARGET;
        private boolean hasHadRotation;
        private volatile int sharedPrefChunkSize = MIN_CHUNK_SIZE;
        private volatile int localPrefChunkSize = MIN_CHUNK_SIZE;
        private volatile int localUpperBufSize;

        private HistogramChunkController(MagazineGroup group, boolean shareable) {
            this.group = group;
            this.shareable = shareable;
            chunkRegistry = group.allocator.chunkRegistry;
        }

        @Override
        public int computeBufferCapacity(
                int requestedSize, int maxCapacity, boolean isReallocation) {
            if (!isReallocation) {
                // Only record allocation size if it's not caused by a reallocation that was triggered by capacity
                // change of the buffer.
                recordAllocationSize(requestedSize);
            }

            // Predict starting capacity from localUpperBufSize, but place limits on the max starting capacity
            // based on the requested size, because localUpperBufSize can potentially be quite large.
            int startCapLimits;
            if (requestedSize <= 32768) { // Less than or equal to 32 KiB.
                startCapLimits = 65536; // Use at most 64 KiB, which is also the AdaptiveRecvByteBufAllocator max.
            } else {
                startCapLimits = requestedSize * 2; // Otherwise use at most twice the requested memory.
            }
            int startingCapacity = Math.min(startCapLimits, localUpperBufSize);
            startingCapacity = Math.max(requestedSize, Math.min(maxCapacity, startingCapacity));
            return startingCapacity;
        }

        private void recordAllocationSize(int bufferSizeToRecord) {
            // Use the preserved size from the reused AdaptiveByteBuf, if available.
            // Otherwise, use the requested buffer size.
            // This way, we better take into account
            if (bufferSizeToRecord == 0) {
                return;
            }
            int bucket = sizeToBucket(bufferSizeToRecord);
            histo[bucket]++;
            if (datumCount++ == datumTarget) {
                rotateHistograms();
            }
        }

        static int sizeToBucket(int size) {
            int index = binarySearchInsertionPoint(Arrays.binarySearch(HISTO_BUCKETS, size));
            return index >= HISTO_BUCKETS.length ? HISTO_BUCKETS.length - 1 : index;
        }

        private static int binarySearchInsertionPoint(int index) {
            if (index < 0) {
                index = -(index + 1);
            }
            return index;
        }

        static int bucketToSize(int sizeBucket) {
            return HISTO_BUCKETS[sizeBucket];
        }

        // ========== 直方图旋转：自适应调整 chunk 大小的核心逻辑 ==========
        private void rotateHistograms() {
            // ========== 步骤 1: 汇总所有 4 个直方图的数据 ==========
            // 维护 4 个滑动直方图，每个记录不同时间段的分配大小分布
            short[][] hs = histos;
            for (int i = 0; i < HISTO_BUCKET_COUNT; i++) {
                // 对每个 bucket，将 4 个直方图中的计数相加
                // & 0xFFFF 是为了处理 short 的符号扩展，确保得到无符号值
                sums[i] = (hs[0][i] & 0xFFFF) + (hs[1][i] & 0xFFFF) + (hs[2][i] & 0xFFFF) + (hs[3][i] & 0xFFFF);
            }
            
            // ========== 步骤 2: 计算总的分配次数 ==========
            // 用于计算百分位数（99-percentile）
            int sum = 0;
            for (int count : sums) {
                sum += count;
            }
            
            // ========== 步骤 3: 找出 99-percentile 对应的 bucket ==========
            // 目标：找到一个大小，使得 99% 的分配都不超过这个大小
            int targetPercentile = (int) (sum * 0.99);
            int sizeBucket = 0;
            for (; sizeBucket < sums.length; sizeBucket++) {
                if (sums[sizeBucket] > targetPercentile) {
                    // 找到了累积分配数超过 99% 阈值的 bucket
                    break;
                }
                targetPercentile -= sums[sizeBucket];
            }
            
            // ========== 步骤 4: 计算首选 chunk 大小 ==========
            // hasHadRotation 标志第一次旋转
            hasHadRotation = true;
            
            // bucketToSize(sizeBucket) 返回该 bucket 对应的大小范围
            // 例如 bucket 5 对应 96KB，bucket 10 对应 512KB
            int percentileSize = bucketToSize(sizeBucket);
            
            // BUFS_PER_CHUNK = 8：每个 chunk 应该能容纳约 8 个这样的分配
            // 这样设计可以减少 chunk 碎片化
            int prefChunkSize = Math.max(percentileSize * BUFS_PER_CHUNK, MIN_CHUNK_SIZE);
            
            // localUpperBufSize 用于预测缓冲区容量
            localUpperBufSize = percentileSize;
            localPrefChunkSize = prefChunkSize;
            
            // ========== 步骤 5: 在共享环境中选择最大的首选大小 ==========
            // 如果多个 magazine 共享这个 controller（shareable=true）
            // 需要确保所有 magazine 都使用一致的 chunk 大小
            if (shareable) {
                // 遍历所有 magazine，找出最大的 localPrefChunkSize
                for (Magazine mag : group.magazines) {
                    HistogramChunkController statistics = (HistogramChunkController) mag.chunkController;
                    prefChunkSize = Math.max(prefChunkSize, statistics.localPrefChunkSize);
                }
            }
            
            // ========== 步骤 6: 根据首选大小是否变化，调整下次旋转的频率 ==========
            if (sharedPrefChunkSize != prefChunkSize) {
                // ========== 大小改变：需要频繁检查（响应工作负载变化） ==========
                // datumTarget 是触发旋转的阈值
                // >> 1 相当于除以 2，增加检查频率
                datumTarget = Math.max(datumTarget >> 1, MIN_DATUM_TARGET);
                // 更新全局首选大小
                sharedPrefChunkSize = prefChunkSize;
            } else {
                // ========== 大小未变：降低检查频率（节省开销） ==========
                // << 1 相当于乘以 2，降低检查频率
                // 上限是 MAX_DATUM_TARGET = 65534
                datumTarget = Math.min(datumTarget << 1, MAX_DATUM_TARGET);
            }

            // ========== 步骤 7: 轮转直方图，清空最旧的直方图 ==========
            // histoIndex 循环在 0-3 之间
            // 每次旋转时，最旧的直方图被清空并重复使用
            histoIndex = histoIndex + 1 & 3;
            histo = histos[histoIndex];
            datumCount = 0;
            // 清空新轮转的直方图
            Arrays.fill(histo, (short) 0);
        }

        /**
         * Get the preferred chunk size, based on statistics from the {@linkplain #recordAllocationSize(int) recorded}
         * allocation sizes.
         * <p>
         * This method must be thread-safe.
         *
         * @return The currently preferred chunk allocation size.
         */
        int preferredChunkSize() {
            return sharedPrefChunkSize;
        }

        @Override
        public void initializeSharedStateIn(ChunkController chunkController) {
            HistogramChunkController statistics = (HistogramChunkController) chunkController;
            int sharedPrefChunkSize = this.sharedPrefChunkSize;
            statistics.localPrefChunkSize = sharedPrefChunkSize;
            statistics.sharedPrefChunkSize = sharedPrefChunkSize;
        }

        @Override
        public Chunk newChunkAllocation(int promptingSize, Magazine magazine) {
            int size = Math.max(promptingSize * BUFS_PER_CHUNK, preferredChunkSize());
            int minChunks = size / MIN_CHUNK_SIZE;
            if (MIN_CHUNK_SIZE * minChunks < size) {
                // Round up to nearest whole MIN_CHUNK_SIZE unit. The MIN_CHUNK_SIZE is an even multiple of many
                // popular small page sizes, like 4k, 16k, and 64k, which makes it easier for the system allocator
                // to manage the memory in terms of whole pages. This reduces memory fragmentation,
                // but without the potentially high overhead that power-of-2 chunk sizes would bring.
                size = MIN_CHUNK_SIZE * (1 + minChunks);
            }

            // Limit chunks to the max size, even if the histogram suggests to go above it.
            size = Math.min(size, MAX_CHUNK_SIZE);

            // If we haven't rotated the histogram yet, optimisticly record this chunk size as our preferred.
            if (!hasHadRotation && sharedPrefChunkSize == MIN_CHUNK_SIZE) {
                sharedPrefChunkSize = size;
            }

            ChunkAllocator chunkAllocator = group.chunkAllocator;
            Chunk chunk = new Chunk(chunkAllocator.allocate(size, size), magazine, true, this);
            chunkRegistry.add(chunk);
            return chunk;
        }

        @Override
        public boolean shouldReleaseChunk(int chunkSize) {
            int preferredSize = preferredChunkSize();
            int givenChunks = chunkSize / MIN_CHUNK_SIZE;
            int preferredChunks = preferredSize / MIN_CHUNK_SIZE;
            int deviation = Math.abs(givenChunks - preferredChunks);

            // Retire chunks with a 5% probability per unit of MIN_CHUNK_SIZE deviation from preference.
            return deviation != 0 &&
                    ThreadLocalRandom.current().nextDouble() * 20.0 < deviation;
        }
    }

    @SuppressJava6Requirement(reason = "Guarded by version check")
    private static final class Magazine {
        private static final AtomicReferenceFieldUpdater<Magazine, Chunk> NEXT_IN_LINE;
        static {
            NEXT_IN_LINE = AtomicReferenceFieldUpdater.newUpdater(Magazine.class, Chunk.class, "nextInLine");
        }
        private static final Chunk MAGAZINE_FREED = new Chunk();

        private static final ObjectPool<AdaptiveByteBuf> EVENT_LOOP_LOCAL_BUFFER_POOL = ObjectPool.newPool(
                new ObjectPool.ObjectCreator<AdaptiveByteBuf>() {
                    @Override
                    public AdaptiveByteBuf newObject(ObjectPool.Handle<AdaptiveByteBuf> handle) {
                        return new AdaptiveByteBuf(handle);
                    }
                });

        private Chunk current;
        @SuppressWarnings("unused") // updated via NEXT_IN_LINE
        private volatile Chunk nextInLine;
        private final MagazineGroup group;
        private final ChunkController chunkController;
        private final AtomicLong usedMemory;
        private final StampedLock allocationLock;
        private final Queue<AdaptiveByteBuf> bufferQueue;
        private final ObjectPool.Handle<AdaptiveByteBuf> handle;
        private final Queue<Chunk> sharedChunkQueue;

        Magazine(MagazineGroup group, boolean shareable, Queue<Chunk> sharedChunkQueue,
                 ChunkController chunkController) {
            this.group = group;
            this.chunkController = chunkController;

            if (shareable) {
                // We only need the StampedLock if this Magazine will be shared across threads.
                allocationLock = new StampedLock();
                bufferQueue = PlatformDependent.newFixedMpmcQueue(MAGAZINE_BUFFER_QUEUE_CAPACITY);
                handle = new ObjectPool.Handle<AdaptiveByteBuf>() {
                    @Override
                    public void recycle(AdaptiveByteBuf self) {
                        bufferQueue.offer(self);
                    }
                };
            } else {
                allocationLock = null;
                bufferQueue = null;
                handle = null;
            }
            usedMemory = new AtomicLong();
            this.sharedChunkQueue = sharedChunkQueue;
        }

        public boolean tryAllocate(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
            if (allocationLock == null) {
                // This magazine is not shared across threads, just allocate directly.
                return allocate(size, maxCapacity, buf, reallocate);
            }

            // Try to retrieve the lock and if successful allocate.
            long writeLock = allocationLock.tryWriteLock();
            if (writeLock != 0) {
                try {
                    return allocate(size, maxCapacity, buf, reallocate);
                } finally {
                    allocationLock.unlockWrite(writeLock);
                }
            }
            return allocateWithoutLock(size, maxCapacity, buf);
        }

        private boolean allocateWithoutLock(int size, int maxCapacity, AdaptiveByteBuf buf) {
            Chunk curr = NEXT_IN_LINE.getAndSet(this, null);
            if (curr == MAGAZINE_FREED) {
                // Allocation raced with a stripe-resize that freed this magazine.
                restoreMagazineFreed();
                return false;
            }
            if (curr == null) {
                curr = sharedChunkQueue.poll();
                if (curr == null) {
                    return false;
                }
                curr.attachToMagazine(this);
            }
            boolean allocated = false;
            int remainingCapacity = curr.remainingCapacity();
            int startingCapacity = chunkController.computeBufferCapacity(
                    size, maxCapacity, true /* never update stats as we don't hold the magazine lock */);
            if (remainingCapacity >= size) {
                curr.readInitInto(buf, size, Math.min(remainingCapacity, startingCapacity), maxCapacity);
                allocated = true;
            }
            try {
                if (remainingCapacity >= RETIRE_CAPACITY) {
                    transferToNextInLineOrRelease(curr);
                    curr = null;
                }
            } finally {
                if (curr != null) {
                    curr.releaseFromMagazine();
                }
            }
            return allocated;
        }

        // ========== Magazine 分配方法：allocate（处理竞争和高性能） ==========
        private boolean allocate(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
            // ========== 步骤 1: 计算初始容量 ==========
            // chunkController 根据申请的大小和历史数据计算合理的初始容量
            // isReallocation=true 表示这是一个扩容操作，可能需要特殊处理
            int startingCapacity = chunkController.computeBufferCapacity(size, maxCapacity, reallocate);
            
            // ========== 步骤 2: 尝试使用当前的 chunk ==========
            // current 是 magazine 正在分配的 chunk，通常有部分空间可用
            Chunk curr = current;
            if (curr != null) {
                // ========== 情况 2a: current chunk 有足够的剩余空间 ==========
                int remainingCapacity = curr.remainingCapacity();
                if (remainingCapacity > startingCapacity) {
                    // 剩余空间 > 初始容量：直接从当前 chunk 分配
                    // 这是最快的路径（fast path），无需获取锁
                    curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                    // 不修改 current，继续为下一个分配使用这个 chunk
                    return true;
                }

                // ========== 情况 2b: current chunk 即将耗尽 ==========
                // 将 current 设为 null，表示即将切换到 next chunk
                current = null;
                
                // 检查剩余空间是否至少满足申请的大小
                if (remainingCapacity >= size) {
                    // 虽然空间不足以满足初始容量需求，但足够满足实际大小需求
                    // 使用剩余的全部空间
                    try {
                        curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
                        return true;
                    } finally {
                        // 无论成功或失败，都要释放这个耗尽的 chunk
                        curr.releaseFromMagazine();
                    }
                }

                // ========== 情况 2c: current chunk 剩余空间不足，检查是否缓存 ==========
                // RETIRE_CAPACITY = 256 字节
                // 如果剩余空间 < 256 字节，认为太小，直接释放（不值得缓存）
                if (remainingCapacity < RETIRE_CAPACITY) {
                    // 释放小于 256 字节的 chunk（无法继续使用）
                    curr.releaseFromMagazine();
                } else {
                    // ========== 缓存即将耗尽的 chunk 到 nextInLine ==========
                    // 这样下一次快速分配失败时，可以快速获得这个 chunk
                    // transferToNextInLineOrRelease 会尝试将 chunk 缓存到 nextInLine
                    // 如果失败（nextInLine 已满），则释放 chunk 或放入共享队列
                    transferToNextInLineOrRelease(curr);
                }
            }

            // ========== 断言：current 已清空 ==========
            assert current == null;
            
            // ========== 步骤 3: 尝试使用 nextInLine 的 chunk ==========
            // nextInLine 是之前缓存的"next"chunk，为即将耗尽 current 做准备
            // 使用原子操作 getAndSet(this, null) 获取并清空
            curr = NEXT_IN_LINE.getAndSet(this, null);
            if (curr != null) {
                // ========== 检查 nextInLine 是否被释放 ==========
                // 在高竞争情况下，magazine 可能被扩容并释放，此时 nextInLine 会被设为 MAGAZINE_FREED
                if (curr == MAGAZINE_FREED) {
                    // Magazine 被释放，恢复这个状态
                    restoreMagazineFreed();
                    return false;
                }

                // ========== 类似的流程：检查 nextInLine chunk 的容量 ==========
                int remainingCapacity = curr.remainingCapacity();
                if (remainingCapacity > startingCapacity) {
                    // nextInLine chunk 有足够空间
                    curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                    current = curr; // 将其升级为 current，供下一次分配使用
                    return true;
                }

                // ========== nextInLine chunk 刚好够用 ==========
                if (remainingCapacity >= size) {
                    try {
                        curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
                        return true;
                    } finally {
                        curr.releaseFromMagazine();
                    }
                } else {
                    // nextInLine chunk 太小，直接释放
                    curr.releaseFromMagazine();
                }
            }

            // ========== 步骤 4: fast path 都失败，尝试从共享队列获取 chunk ==========
            // sharedChunkQueue 是一个全局的、所有 magazine 共享的队列
            // 存放被其他 magazine 释放但仍可用的 chunk
            curr = sharedChunkQueue.poll();
            if (curr == null) {
                // 共享队列为空，创建全新的 chunk
                // chunkController.newChunkAllocation 会根据历史数据决定 chunk 大小
                curr = chunkController.newChunkAllocation(size, this);
            } else {
                // ========== 从共享队列获取的 chunk 需要重新附加到这个 magazine ==========
                curr.attachToMagazine(this);

                // ========== 检查获取的 chunk 是否足够大 ==========
                int remainingCapacity = curr.remainingCapacity();
                if (remainingCapacity < size) {
                    // 这个 chunk 太小，无法满足申请
                    if (remainingCapacity < RETIRE_CAPACITY) {
                        // 太小了，直接释放
                        curr.releaseFromMagazine();
                    } else {
                        // 还可以缓存，放入 nextInLine
                        transferToNextInLineOrRelease(curr);
                    }
                    // 创建新的 chunk 来满足这个分配请求
                    curr = chunkController.newChunkAllocation(size, this);
                }
            }

            // ========== 步骤 5: 使用获取的 chunk（新建或共享队列中获取） ==========
            // 将 curr 设为 current，标记为"当前正在使用"的 chunk
            current = curr;
            try {
                // ========== 最后的分配尝试 ==========
                int remainingCapacity = curr.remainingCapacity();
                // 由于 curr 是新建或重新选择的，应该有足够空间
                assert remainingCapacity >= size;
                
                // 根据剩余空间决定是否使用初始容量
                if (remainingCapacity > startingCapacity) {
                    // 用初始容量（受历史数据影响，可能小于剩余空间）
                    curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                    curr = null; // 标记已成功使用，不需要在 finally 中释放
                } else {
                    // 用全部剩余空间（chunk 会被彻底耗尽）
                    curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
                }
            } finally {
                // ========== 安全清理 ==========
                // 如果 curr 仍不为 null，说明初始化失败，需要释放和重置状态
                if (curr != null) {
                    // readInitInto(...) 失败时需要清理
                    curr.releaseFromMagazine();
                    current = null; // 重置 current
                }
            }
            return true;
        }

        private void restoreMagazineFreed() {
            Chunk next = NEXT_IN_LINE.getAndSet(this, MAGAZINE_FREED);
            if (next != null && next != MAGAZINE_FREED) {
                // A chunk snuck in through a race. Release it after restoring MAGAZINE_FREED state.
                next.releaseFromMagazine();
            }
        }

        private void transferToNextInLineOrRelease(Chunk chunk) {
            if (NEXT_IN_LINE.compareAndSet(this, null, chunk)) {
                return;
            }

            Chunk nextChunk = NEXT_IN_LINE.get(this);
            if (nextChunk != null && nextChunk != MAGAZINE_FREED
                    && chunk.remainingCapacity() > nextChunk.remainingCapacity()) {
                if (NEXT_IN_LINE.compareAndSet(this, nextChunk, chunk)) {
                    nextChunk.releaseFromMagazine();
                    return;
                }
            }
            // Next-in-line is occupied. We don't try to add it to the central queue yet as it might still be used
            // by some buffers and so is attached to a Magazine.
            // Once a Chunk is completely released by Chunk.release() it will try to move itself to the queue
            // as last resort.
            chunk.releaseFromMagazine();
        }

        boolean trySetNextInLine(Chunk chunk) {
            return NEXT_IN_LINE.compareAndSet(this, null, chunk);
        }

        void free() {
            // Release the current Chunk and the next that was stored for later usage.
            restoreMagazineFreed();
            long stamp = allocationLock != null ? allocationLock.writeLock() : 0;
            try {
                if (current != null) {
                    current.releaseFromMagazine();
                    current = null;
                }
            } finally {
                if (allocationLock != null) {
                    allocationLock.unlockWrite(stamp);
                }
            }
        }

        public AdaptiveByteBuf newBuffer() {
            AdaptiveByteBuf buf;
            if (handle == null) {
                buf = EVENT_LOOP_LOCAL_BUFFER_POOL.get();
            } else {
                buf = bufferQueue.poll();
                if (buf == null) {
                    buf = new AdaptiveByteBuf(handle);
                }
            }
            buf.resetRefCnt();
            buf.discardMarks();
            return buf;
        }

        boolean offerToQueue(Chunk chunk) {
            return group.offerToQueue(chunk);
        }

        public void initializeSharedStateIn(Magazine other) {
            chunkController.initializeSharedStateIn(other.chunkController);
        }
    }

    private static class Chunk implements ReferenceCounted {
        private static final long REFCNT_FIELD_OFFSET =
                ReferenceCountUpdater.getUnsafeOffset(Chunk.class, "refCnt");
        private static final AtomicIntegerFieldUpdater<Chunk> AIF_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "refCnt");

        protected final AbstractByteBuf delegate;
        protected Magazine magazine;
        private final AdaptivePoolingAllocator allocator;
        private final ChunkReleasePredicate chunkReleasePredicate;
        private final int capacity;
        private final boolean pooled;
        protected int allocatedBytes;

        private static final ReferenceCountUpdater<Chunk> updater =
                new ReferenceCountUpdater<Chunk>() {
                    @Override
                    protected AtomicIntegerFieldUpdater<Chunk> updater() {
                        return AIF_UPDATER;
                    }
                    @Override
                    protected long unsafeOffset() {
                        // on native image, REFCNT_FIELD_OFFSET can be recomputed even with Unsafe unavailable, so we
                        // need to guard here
                        return PlatformDependent.hasUnsafe() ? REFCNT_FIELD_OFFSET : -1;
                    }
                };

        // Value might not equal "real" reference count, all access should be via the updater
        @SuppressWarnings({"unused", "FieldMayBeFinal"})
        private volatile int refCnt;

        Chunk() {
            // Constructor only used by the MAGAZINE_FREED sentinel.
            delegate = null;
            magazine = null;
            allocator = null;
            chunkReleasePredicate = null;
            capacity = 0;
            pooled = false;
        }

        Chunk(AbstractByteBuf delegate, Magazine magazine, boolean pooled,
              ChunkReleasePredicate chunkReleasePredicate) {
            this.delegate = delegate;
            this.pooled = pooled;
            capacity = delegate.capacity();
            updater.setInitialValue(this);
            attachToMagazine(magazine);

            // We need the top-level allocator so ByteBuf.capacity(int) can call reallocate()
            allocator = magazine.group.allocator;

            this.chunkReleasePredicate = chunkReleasePredicate;
        }

        Magazine currentMagazine()  {
            return magazine;
        }

        void detachFromMagazine() {
            if (magazine != null) {
                magazine.usedMemory.getAndAdd(-capacity);
                magazine = null;
            }
        }

        void attachToMagazine(Magazine magazine) {
            assert this.magazine == null;
            this.magazine = magazine;
            magazine.usedMemory.getAndAdd(capacity);
        }

        @Override
        public Chunk touch(Object hint) {
            return this;
        }

        @Override
        public int refCnt() {
            return updater.refCnt(this);
        }

        @Override
        public Chunk retain() {
            return updater.retain(this);
        }

        @Override
        public Chunk retain(int increment) {
            return updater.retain(this, increment);
        }

        @Override
        public Chunk touch() {
            return this;
        }

        @Override
        public boolean release() {
            if (updater.release(this)) {
                deallocate();
                return true;
            }
            return false;
        }

        @Override
        public boolean release(int decrement) {
            if (updater.release(this, decrement)) {
                deallocate();
                return true;
            }
            return false;
        }

        /**
         * Called when a magazine is done using this chunk, probably because it was emptied.
         */
        boolean releaseFromMagazine() {
            return release();
        }

        /**
         * Called when a ByteBuf is done using its allocation in this chunk.
         */
        boolean releaseSegment(int ignoredSegmentId) {
            return release();
        }

        private void deallocate() {
            Magazine mag = magazine;
            int chunkSize = delegate.capacity();
            if (!pooled || chunkReleasePredicate.shouldReleaseChunk(chunkSize) || mag == null) {
                // Drop the chunk if the parent allocator is closed,
                // or if the chunk deviates too much from the preferred chunk size.
                detachFromMagazine();
                allocator.chunkRegistry.remove(this);
                delegate.release();
            } else {
                updater.resetRefCnt(this);
                delegate.setIndex(0, 0);
                allocatedBytes = 0;
                if (!mag.trySetNextInLine(this)) {
                    // As this Chunk does not belong to the mag anymore we need to decrease the used memory .
                    detachFromMagazine();
                    if (!mag.offerToQueue(this)) {
                        // The central queue is full. Ensure we release again as we previously did use resetRefCnt()
                        // which did increase the reference count by 1.
                        boolean released = updater.release(this);
                        allocator.chunkRegistry.remove(this);
                        delegate.release();
                        assert released;
                    }
                }
            }
        }

        public void readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            int startIndex = allocatedBytes;
            allocatedBytes = startIndex + startingCapacity;
            Chunk chunk = this;
            chunk.retain();
            try {
                buf.init(delegate, chunk, 0, 0, startIndex, size, startingCapacity, maxCapacity, 0);
                chunk = null;
            } finally {
                if (chunk != null) {
                    // If chunk is not null we know that buf.init(...) failed and so we need to manually release
                    // the chunk again as we retained it before calling buf.init(...). Beside this we also need to
                    // restore the old allocatedBytes value.
                    allocatedBytes = startIndex;
                    chunk.release();
                }
            }
        }

        public int remainingCapacity() {
            return capacity - allocatedBytes;
        }

        public int capacity() {
            return capacity;
        }
    }

    private static final class SizeClassedChunk extends Chunk {
        private static final int FREE_LIST_EMPTY = -1;
        private final int segmentSize;
        private final MpscIntQueue freeList;

        SizeClassedChunk(AbstractByteBuf delegate, Magazine magazine, boolean pooled, int segmentSize,
                         ChunkReleasePredicate shouldReleaseChunk) {
            super(delegate, magazine, pooled, shouldReleaseChunk);
            int capacity = delegate.capacity();
            this.segmentSize = segmentSize;
            int segmentCount = capacity / segmentSize;
            assert segmentCount > 0: "Chunk must have a positive number of segments";
            freeList = new MpscAtomicIntegerArrayQueue(segmentCount, FREE_LIST_EMPTY);
            freeList.fill(segmentCount, new IntSupplier() {
                int counter;
                @Override
                public int get() {
                    return counter++;
                }
            });
        }

        @Override
        public void readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            int segmentId = freeList.poll();
            if (segmentId == FREE_LIST_EMPTY) {
                throw new IllegalStateException("Free list is empty");
            }

            int startIndex = segmentId * segmentSize;
            allocatedBytes += segmentSize;
            Chunk chunk = this;
            chunk.retain();
            try {
                buf.init(delegate, chunk, 0, 0, startIndex, size, startingCapacity, maxCapacity, segmentId);
                chunk = null;
            } finally {
                if (chunk != null) {
                    // If chunk is not null we know that buf.init(...) failed and so we need to manually release
                    // the chunk again as we retained it before calling buf.init(...). Beside this we also need to
                    // restore the old allocatedBytes value.
                    allocatedBytes -= segmentSize;
                    chunk.releaseSegment(segmentId);
                }
            }
        }

        @Override
        public int remainingCapacity() {
            int remainingCapacity = super.remainingCapacity();
            if (remainingCapacity > segmentSize) {
                return remainingCapacity;
            }
            int updatedRemainingCapacity = freeList.size() * segmentSize;
            if (updatedRemainingCapacity == remainingCapacity) {
                return remainingCapacity;
            }
            // update allocatedBytes based on what's available in the free list
            allocatedBytes = capacity() - updatedRemainingCapacity;
            return updatedRemainingCapacity;
        }

        @Override
        boolean releaseFromMagazine() {
            // Size-classed chunks can be reused before they become empty.
            // We can therefor put them in the shared queue as soon as the magazine is done with this chunk.
            Magazine mag = magazine;
            detachFromMagazine();
            if (!mag.offerToQueue(this)) {
                return super.releaseFromMagazine();
            }
            return false;
        }

        @Override
        boolean releaseSegment(int segmentId) {
            boolean released = release();
            boolean segmentReturned = freeList.offer(segmentId);
            assert segmentReturned: "Unable to return segment " + segmentId + " to free list";
            return released;
        }
    }

    static final class AdaptiveByteBuf extends AbstractReferenceCountedByteBuf {

        private final ObjectPool.Handle<AdaptiveByteBuf> handle;

        private int adjustment;
        private AbstractByteBuf rootParent;
        Chunk chunk;
        private int length;
        private int maxFastCapacity;
        private int segmentId;
        private ByteBuffer tmpNioBuf;
        private boolean hasArray;
        private boolean hasMemoryAddress;

        AdaptiveByteBuf(ObjectPool.Handle<AdaptiveByteBuf> recyclerHandle) {
            super(0);
            handle = ObjectUtil.checkNotNull(recyclerHandle, "recyclerHandle");
        }

        void init(AbstractByteBuf unwrapped, Chunk wrapped, int readerIndex, int writerIndex,
                  int adjustment, int size, int capacity, int maxCapacity, int segmentId) {
            this.adjustment = adjustment;
            chunk = wrapped;
            length = size;
            maxFastCapacity = capacity;
            maxCapacity(maxCapacity);
            setIndex0(readerIndex, writerIndex);
            this.segmentId = segmentId;
            hasArray = unwrapped.hasArray();
            hasMemoryAddress = unwrapped.hasMemoryAddress();
            rootParent = unwrapped;
            tmpNioBuf = null;
        }

        private AbstractByteBuf rootParent() {
            final AbstractByteBuf rootParent = this.rootParent;
            if (rootParent != null) {
                return rootParent;
            }
            throw new IllegalReferenceCountException();
        }

        @Override
        public int capacity() {
            return length;
        }

        @Override
        public int maxFastWritableBytes() {
            return Math.min(maxFastCapacity, maxCapacity()) - writerIndex;
        }

        @Override
        public ByteBuf capacity(int newCapacity) {
            if (length <= newCapacity && newCapacity <= maxFastCapacity) {
                ensureAccessible();
                length = newCapacity;
                return this;
            }
            checkNewCapacity(newCapacity);
            if (newCapacity < capacity()) {
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }

            // Reallocation required.
            Chunk chunk = this.chunk;
            AdaptivePoolingAllocator allocator = chunk.allocator;
            int readerIndex = this.readerIndex;
            int writerIndex = this.writerIndex;
            int baseOldRootIndex = adjustment;
            int oldCapacity = length;
            int oldSegmentId = segmentId;
            AbstractByteBuf oldRoot = rootParent();
            allocator.reallocate(newCapacity, maxCapacity(), this);
            oldRoot.getBytes(baseOldRootIndex, this, 0, oldCapacity);
            chunk.releaseSegment(oldSegmentId);
            this.readerIndex = readerIndex;
            this.writerIndex = writerIndex;
            return this;
        }

        @Override
        public ByteBufAllocator alloc() {
            return rootParent().alloc();
        }

        @Override
        public ByteOrder order() {
            return rootParent().order();
        }

        @Override
        public ByteBuf unwrap() {
            return null;
        }

        @Override
        public boolean isDirect() {
            return rootParent().isDirect();
        }

        @Override
        public int arrayOffset() {
            return idx(rootParent().arrayOffset());
        }

        @Override
        public boolean hasMemoryAddress() {
            return hasMemoryAddress;
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return rootParent().memoryAddress() + adjustment;
        }

        @Override
        public ByteBuffer nioBuffer(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffer(idx(index), length);
        }

        @Override
        public ByteBuffer internalNioBuffer(int index, int length) {
            checkIndex(index, length);
            return (ByteBuffer) internalNioBuffer().position(index).limit(index + length);
        }

        private ByteBuffer internalNioBuffer() {
            if (tmpNioBuf == null) {
                tmpNioBuf = rootParent().nioBuffer(adjustment, maxFastCapacity);
            }
            return (ByteBuffer) tmpNioBuf.clear();
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffers(idx(index), length);
        }

        @Override
        public boolean hasArray() {
            return hasArray;
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            return rootParent().array();
        }

        @Override
        public ByteBuf copy(int index, int length) {
            checkIndex(index, length);
            return rootParent().copy(idx(index), length);
        }

        @Override
        public int nioBufferCount() {
            return rootParent().nioBufferCount();
        }

        @Override
        protected byte _getByte(int index) {
            return rootParent()._getByte(idx(index));
        }

        @Override
        protected short _getShort(int index) {
            return rootParent()._getShort(idx(index));
        }

        @Override
        protected short _getShortLE(int index) {
            return rootParent()._getShortLE(idx(index));
        }

        @Override
        protected int _getUnsignedMedium(int index) {
            return rootParent()._getUnsignedMedium(idx(index));
        }

        @Override
        protected int _getUnsignedMediumLE(int index) {
            return rootParent()._getUnsignedMediumLE(idx(index));
        }

        @Override
        protected int _getInt(int index) {
            return rootParent()._getInt(idx(index));
        }

        @Override
        protected int _getIntLE(int index) {
            return rootParent()._getIntLE(idx(index));
        }

        @Override
        protected long _getLong(int index) {
            return rootParent()._getLong(idx(index));
        }

        @Override
        protected long _getLongLE(int index) {
            return rootParent()._getLongLE(idx(index));
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuffer dst) {
            checkIndex(index, dst.remaining());
            rootParent().getBytes(idx(index), dst);
            return this;
        }

        @Override
        protected void _setByte(int index, int value) {
            rootParent()._setByte(idx(index), value);
        }

        @Override
        protected void _setShort(int index, int value) {
            rootParent()._setShort(idx(index), value);
        }

        @Override
        protected void _setShortLE(int index, int value) {
            rootParent()._setShortLE(idx(index), value);
        }

        @Override
        protected void _setMedium(int index, int value) {
            rootParent()._setMedium(idx(index), value);
        }

        @Override
        protected void _setMediumLE(int index, int value) {
            rootParent()._setMediumLE(idx(index), value);
        }

        @Override
        protected void _setInt(int index, int value) {
            rootParent()._setInt(idx(index), value);
        }

        @Override
        protected void _setIntLE(int index, int value) {
            rootParent()._setIntLE(idx(index), value);
        }

        @Override
        protected void _setLong(int index, long value) {
            rootParent()._setLong(idx(index), value);
        }

        @Override
        protected void _setLongLE(int index, long value) {
            rootParent().setLongLE(idx(index), value);
        }

        @Override
        public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
            checkIndex(index, length);
            ByteBuffer tmp = (ByteBuffer) internalNioBuffer().clear().position(index);
            tmp.put(src, srcIndex, length);
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
            checkIndex(index, length);
            ByteBuffer tmp = (ByteBuffer) internalNioBuffer().clear().position(index);
            tmp.put(src.nioBuffer(srcIndex, length));
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuffer src) {
            checkIndex(index, src.remaining());
            ByteBuffer tmp = (ByteBuffer) internalNioBuffer().clear().position(index);
            tmp.put(src);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, OutputStream out, int length)
                throws IOException {
            checkIndex(index, length);
            if (length != 0) {
                ByteBufUtil.readBytes(alloc(), internalNioBuffer().duplicate(), index, length, out);
            }
            return this;
        }

        @Override
        public int getBytes(int index, GatheringByteChannel out, int length)
                throws IOException {
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf);
        }

        @Override
        public int getBytes(int index, FileChannel out, long position, int length)
                throws IOException {
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf, position);
        }

        @Override
        public int setBytes(int index, InputStream in, int length)
                throws IOException {
            checkIndex(index, length);
            final AbstractByteBuf rootParent = rootParent();
            if (rootParent.hasArray()) {
                return rootParent.setBytes(idx(index), in, length);
            }
            byte[] tmp = ByteBufUtil.threadLocalTempArray(length);
            int readBytes = in.read(tmp, 0, length);
            if (readBytes <= 0) {
                return readBytes;
            }
            setBytes(index, tmp, 0, readBytes);
            return readBytes;
        }

        @Override
        public int setBytes(int index, ScatteringByteChannel in, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length).duplicate());
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setBytes(int index, FileChannel in, long position, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length).duplicate(), position);
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setCharSequence(int index, CharSequence sequence, Charset charset) {
            return setCharSequence0(index, sequence, charset, false);
        }

        private int setCharSequence0(int index, CharSequence sequence, Charset charset, boolean expand) {
            if (charset.equals(CharsetUtil.UTF_8)) {
                int length = ByteBufUtil.utf8MaxBytes(sequence);
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                // Directly pass in the rootParent() with the adjusted index
                return ByteBufUtil.writeUtf8(rootParent(), idx(index), length, sequence, sequence.length());
            }
            if (charset.equals(CharsetUtil.US_ASCII) || charset.equals(CharsetUtil.ISO_8859_1)) {
                int length = sequence.length();
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                // Directly pass in the rootParent() with the adjusted index
                return ByteBufUtil.writeAscii(rootParent(), idx(index), sequence, length);
            }
            byte[] bytes = sequence.toString().getBytes(charset);
            if (expand) {
                ensureWritable0(bytes.length);
                // setBytes(...) will take care of checking the indices.
            }
            setBytes(index, bytes);
            return bytes.length;
        }

        @Override
        public int writeCharSequence(CharSequence sequence, Charset charset) {
            int written = setCharSequence0(writerIndex, sequence, charset, true);
            writerIndex += written;
            return written;
        }

        @Override
        public int forEachByte(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByte(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public int forEachByteDesc(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByteDesc(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public ByteBuf setZero(int index, int length) {
            checkIndex(index, length);
            rootParent().setZero(idx(index), length);
            return this;
        }

        @Override
        public ByteBuf writeZero(int length) {
            ensureWritable(length);
            rootParent().setZero(idx(writerIndex), length);
            writerIndex += length;
            return this;
        }

        private int forEachResult(int ret) {
            if (ret < adjustment) {
                return -1;
            }
            return ret - adjustment;
        }

        @Override
        public boolean isContiguous() {
            return rootParent().isContiguous();
        }

        private int idx(int index) {
            return index + adjustment;
        }

        @Override
        protected void deallocate() {
            if (chunk != null) {
                chunk.releaseSegment(segmentId);
            }
            tmpNioBuf = null;
            chunk = null;
            rootParent = null;
            if (handle instanceof EnhancedHandle) {
                EnhancedHandle<AdaptiveByteBuf>  enhancedHandle = (EnhancedHandle<AdaptiveByteBuf>) handle;
                enhancedHandle.unguardedRecycle(this);
            } else {
                handle.recycle(this);
            }
        }
    }

    /**
     * The strategy for how {@link AdaptivePoolingAllocator} should allocate chunk buffers.
     */
    interface ChunkAllocator {
        /**
         * Allocate a buffer for a chunk. This can be any kind of {@link AbstractByteBuf} implementation.
         * @param initialCapacity The initial capacity of the returned {@link AbstractByteBuf}.
         * @param maxCapacity The maximum capacity of the returned {@link AbstractByteBuf}.
         * @return The buffer that represents the chunk memory.
         */
        AbstractByteBuf allocate(int initialCapacity, int maxCapacity);
    }
}
