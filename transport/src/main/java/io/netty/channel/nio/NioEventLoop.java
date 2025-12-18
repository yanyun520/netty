/*
 * Copyright 2012 The Netty Project
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
package io.netty.channel.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopException;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.IntSupplier;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReflectionUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;

import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SingleThreadEventLoop} implementation which register the {@link Channel}'s to a
 * {@link Selector} and so does the multi-plexing of these in the event loop.
 *
 */
public final class NioEventLoop extends SingleThreadEventLoop {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioEventLoop.class);

    private static final int CLEANUP_INTERVAL = 256; // XXX Hard-coded value, but won't need customization.

    private static final boolean DISABLE_KEY_SET_OPTIMIZATION =
            SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);

    private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
    private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;

    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            return selectNow();
        }
    };

    // Workaround for JDK NIO bug.
    //
    // See:
    // - https://bugs.openjdk.java.net/browse/JDK-6427854 for first few dev (unreleased) builds of JDK 7
    // - https://bugs.openjdk.java.net/browse/JDK-6527572 for JDK prior to 5.0u15-rev and 6u10
    // - https://github.com/netty/netty/issues/203
    static {
        // 检查Java版本是否小于7
        if (PlatformDependent.javaVersion() < 7) {
            // 定义系统属性键
            final String key = "sun.nio.ch.bugLevel";
            // 获取系统属性值
            final String bugLevel = SystemPropertyUtil.get(key);
            // 如果系统属性值为空
            if (bugLevel == null) {
                try {
                    // 使用AccessController设置系统属性
                    AccessController.doPrivileged(new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            // 设置系统属性值为空字符串
                            System.setProperty(key, "");
                            return null;
                        }
                    });
                } catch (final SecurityException e) {
                    // 如果捕获到SecurityException异常，记录日志
                    logger.debug("Unable to get/set System Property: " + key, e);
                }
            }
        }

        // 获取系统属性"io.netty.selectorAutoRebuildThreshold"的值，默认为512
        int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
        // 如果值小于MIN_PREMATURE_SELECTOR_RETURNS，则设置为0
        if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
            selectorAutoRebuildThreshold = 0;
        }

        // 设置SELECTOR_AUTO_REBUILD_THRESHOLD的值
        SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;

        // 如果开启了调试日志
        if (logger.isDebugEnabled()) {
            // 记录日志：系统属性-Dio.netty.noKeySetOptimization的值
            logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEY_SET_OPTIMIZATION);
            // 记录日志：系统属性-Dio.netty.selectorAutoRebuildThreshold的值
            logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
        }
    }

    /**
     * The NIO {@link Selector}.
     */
    private Selector selector;
    private Selector unwrappedSelector;
    private SelectedSelectionKeySet selectedKeys;

    private final SelectorProvider provider;

    private static final long AWAKE = -1L;
    private static final long NONE = Long.MAX_VALUE;

    // nextWakeupNanos is:
    //    AWAKE            when EL is awake
    //    NONE             when EL is waiting with no wakeup scheduled
    //    other value T    when EL is waiting with wakeup scheduled at time T
    private final AtomicLong nextWakeupNanos = new AtomicLong(AWAKE);

    private final SelectStrategy selectStrategy;

    private volatile int ioRatio = 50;
    private int cancelledKeys;
    private boolean needsToSelectAgain;

    NioEventLoop(NioEventLoopGroup parent, Executor executor, SelectorProvider selectorProvider,
                 SelectStrategy strategy, RejectedExecutionHandler rejectedExecutionHandler,
                 EventLoopTaskQueueFactory taskQueueFactory, EventLoopTaskQueueFactory tailTaskQueueFactory) {
        super(parent, executor, false, newTaskQueue(taskQueueFactory), newTaskQueue(tailTaskQueueFactory),
                rejectedExecutionHandler);
        this.provider = ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
        this.selectStrategy = ObjectUtil.checkNotNull(strategy, "selectStrategy");
        final SelectorTuple selectorTuple = openSelector();
        this.selector = selectorTuple.selector;
        this.unwrappedSelector = selectorTuple.unwrappedSelector;
    }

    private static Queue<Runnable> newTaskQueue(
            EventLoopTaskQueueFactory queueFactory) {
        if (queueFactory == null) {
            return newTaskQueue0(DEFAULT_MAX_PENDING_TASKS);
        }
        return queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS);
    }

    private static final class SelectorTuple {
        final Selector unwrappedSelector;
        final Selector selector;

        SelectorTuple(Selector unwrappedSelector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = unwrappedSelector;
        }

        SelectorTuple(Selector unwrappedSelector, Selector selector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = selector;
        }
    }

    private SelectorTuple openSelector() {
        final Selector unwrappedSelector;
        try {
            unwrappedSelector = provider.openSelector();
        } catch (IOException e) {
            throw new ChannelException("failed to open a new selector", e);
        }

        // 关键判断：是否禁用优化
        if (DISABLE_KEY_SET_OPTIMIZATION) {
            return new SelectorTuple(unwrappedSelector);
        }



        Object maybeSelectorImplClass = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return Class.forName(
                            "sun.nio.ch.SelectorImpl",
                            false,
                            PlatformDependent.getSystemClassLoader());
                } catch (Throwable cause) {
                    return cause;
                }
            }
        });

        if (!(maybeSelectorImplClass instanceof Class) ||
            // ensure the current selector implementation is what we can instrument.
            !((Class<?>) maybeSelectorImplClass).isAssignableFrom(unwrappedSelector.getClass())) {
            if (maybeSelectorImplClass instanceof Throwable) {
                Throwable t = (Throwable) maybeSelectorImplClass;
                logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, t);
            }
            return new SelectorTuple(unwrappedSelector);
        }


        // ... 反射获取 SelectorImpl 类 ...
        final Class<?> selectorImplClass = (Class<?>) maybeSelectorImplClass;
        // 创建自定义的 SelectedSelectionKeySet（数组实现）
        final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

        // 使用反射或 Unsafe 替换 Selector 内部的 selectedKeys 字段
        Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                    Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

                    if (PlatformDependent.javaVersion() >= 9 && PlatformDependent.hasUnsafe()) {
                        // Java 9+ 使用 Unsafe 直接修改内存
                        // Let us try to use sun.misc.Unsafe to replace the SelectionKeySet.
                        // This allows us to also do this in Java9+ without any extra flags.
                        long selectedKeysFieldOffset = PlatformDependent.objectFieldOffset(selectedKeysField);
                        long publicSelectedKeysFieldOffset =
                                PlatformDependent.objectFieldOffset(publicSelectedKeysField);

                        if (selectedKeysFieldOffset != -1 && publicSelectedKeysFieldOffset != -1) {
                            PlatformDependent.putObject(
                                    unwrappedSelector, selectedKeysFieldOffset, selectedKeySet);
                            PlatformDependent.putObject(
                                    unwrappedSelector, publicSelectedKeysFieldOffset, selectedKeySet);
                            return null;
                        }
                        // We could not retrieve the offset, lets try reflection as last-resort.
                    }


                    // Java 8 或 Unsafe 不可用时，使用反射
                    Throwable cause = ReflectionUtil.trySetAccessible(selectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }
                    cause = ReflectionUtil.trySetAccessible(publicSelectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }

                    selectedKeysField.set(unwrappedSelector, selectedKeySet);
                    publicSelectedKeysField.set(unwrappedSelector, selectedKeySet);
                    return null;
                } catch (NoSuchFieldException e) {
                    return e;
                } catch (IllegalAccessException e) {
                    return e;
                }
            }
        });

        if (maybeException instanceof Exception) {
            selectedKeys = null; // ← 如果优化失败，设置为 null
            Exception e = (Exception) maybeException;
            logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, e);
            return new SelectorTuple(unwrappedSelector);
        }
        // ← 优化成功：赋值给 NioEventLoop 的成员变量
        selectedKeys = selectedKeySet;
        logger.trace("instrumented a special java.util.Set into: {}", unwrappedSelector);
        return new SelectorTuple(unwrappedSelector,
                                 new SelectedSelectionKeySetSelector(unwrappedSelector, selectedKeySet));
    }

    /**
     * Returns the {@link SelectorProvider} used by this {@link NioEventLoop} to obtain the {@link Selector}.
     */
    public SelectorProvider selectorProvider() {
        return provider;
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    private static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    /**
     * Registers an arbitrary {@link SelectableChannel}, not necessarily created by Netty, to the {@link Selector}
     * of this event loop.  Once the specified {@link SelectableChannel} is registered, the specified {@code task} will
     * be executed by this event loop when the {@link SelectableChannel} is ready.
     */
    public void register(final SelectableChannel ch, final int interestOps, final NioTask<?> task) {
        ObjectUtil.checkNotNull(ch, "ch");
        if (interestOps == 0) {
            throw new IllegalArgumentException("interestOps must be non-zero.");
        }
        if ((interestOps & ~ch.validOps()) != 0) {
            throw new IllegalArgumentException(
                    "invalid interestOps: " + interestOps + "(validOps: " + ch.validOps() + ')');
        }
        ObjectUtil.checkNotNull(task, "task");

        if (isShutdown()) {
            throw new IllegalStateException("event loop shut down");
        }

        if (inEventLoop()) {
            register0(ch, interestOps, task);
        } else {
            try {
                // Offload to the EventLoop as otherwise java.nio.channels.spi.AbstractSelectableChannel.register
                // may block for a long time while trying to obtain an internal lock that may be hold while selecting.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        register0(ch, interestOps, task);
                    }
                }).sync();
            } catch (InterruptedException ignore) {
                // Even if interrupted we did schedule it so just mark the Thread as interrupted.
                Thread.currentThread().interrupt();
            }
        }
    }

    private void register0(SelectableChannel ch, int interestOps, NioTask<?> task) {
        try {
            ch.register(unwrappedSelector, interestOps, task);
        } catch (Exception e) {
            throw new EventLoopException("failed to register a channel", e);
        }
    }

    /**
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    public int getIoRatio() {
        return ioRatio;
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop. Value range from 1-100.
     * The default value is {@code 50}, which means the event loop will try to spend the same amount of time for I/O
     * as for non-I/O tasks. The lower the number the more time can be spent on non-I/O tasks. If value set to
     * {@code 100}, this feature will be disabled and event loop will not attempt to balance I/O and non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)");
        }
        this.ioRatio = ioRatio;
    }

    /**
     * Replaces the current {@link Selector} of this event loop with newly created {@link Selector}s to work
     * around the infamous epoll 100% CPU bug.
     */
    public void rebuildSelector() {
        if (!inEventLoop()) {
            execute(new Runnable() {
                @Override
                public void run() {
                    rebuildSelector0();
                }
            });
            return;
        }
        rebuildSelector0();
    }

    @Override
    public int registeredChannels() {
        return selector.keys().size() - cancelledKeys;
    }

    @Override
    public Iterator<Channel> registeredChannelsIterator() {
        assert inEventLoop();
        final Set<SelectionKey> keys = selector.keys();
        if (keys.isEmpty()) {
            return ChannelsReadOnlyIterator.empty();
        }
        return new Iterator<Channel>() {
            final Iterator<SelectionKey> selectionKeyIterator =
                    ObjectUtil.checkNotNull(keys, "selectionKeys")
                            .iterator();
            Channel next;
            boolean isDone;

            @Override
            public boolean hasNext() {
                if (isDone) {
                    return false;
                }
                Channel cur = next;
                if (cur == null) {
                    cur = next = nextOrDone();
                    return cur != null;
                }
                return true;
            }

            @Override
            public Channel next() {
                if (isDone) {
                    throw new NoSuchElementException();
                }
                Channel cur = next;
                if (cur == null) {
                    cur = nextOrDone();
                    if (cur == null) {
                        throw new NoSuchElementException();
                    }
                }
                next = nextOrDone();
                return cur;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }

            private Channel nextOrDone() {
                Iterator<SelectionKey> it = selectionKeyIterator;
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    if (key.isValid()) {
                        Object attachment = key.attachment();
                        if (attachment instanceof AbstractNioChannel) {
                            return (AbstractNioChannel) attachment;
                        }
                    }
                }
                isDone = true;
                return null;
            }
        };
    }

    private void rebuildSelector0() {
        final Selector oldSelector = selector;
        final SelectorTuple newSelectorTuple;

        if (oldSelector == null) {
            return;
        }

        try {
            newSelectorTuple = openSelector();
        } catch (Exception e) {
            logger.warn("Failed to create a new Selector.", e);
            return;
        }

        // Register all channels to the new Selector.
        int nChannels = 0;
        for (SelectionKey key: oldSelector.keys()) {
            Object a = key.attachment();
            try {
                if (!key.isValid() || key.channel().keyFor(newSelectorTuple.unwrappedSelector) != null) {
                    continue;
                }

                int interestOps = key.interestOps();
                key.cancel();
                SelectionKey newKey = key.channel().register(newSelectorTuple.unwrappedSelector, interestOps, a);
                if (a instanceof AbstractNioChannel) {
                    // Update SelectionKey
                    ((AbstractNioChannel) a).selectionKey = newKey;
                }
                nChannels ++;
            } catch (Exception e) {
                logger.warn("Failed to re-register a Channel to the new Selector.", e);
                if (a instanceof AbstractNioChannel) {
                    AbstractNioChannel ch = (AbstractNioChannel) a;
                    ch.unsafe().close(ch.unsafe().voidPromise());
                } else {
                    @SuppressWarnings("unchecked")
                    NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                    invokeChannelUnregistered(task, key, e);
                }
            }
        }

        selector = newSelectorTuple.selector;
        unwrappedSelector = newSelectorTuple.unwrappedSelector;

        try {
            // time to close the old selector as everything else is registered to the new one
            oldSelector.close();
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close the old Selector.", t);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
        }
    }

    @Override
    protected void run() {
        // 计数器：用于检测 select() 是否频繁过早返回（JDK bug 检测机制）
        // 当 select() 返回但没有就绪事件时，selectCnt 会递增，用于后续判断是否需要重建 Selector
        int selectCnt = 0;
        
        // 无限循环：EventLoop 的核心事件循环，只有在 shutdown 时才会退出
        for (;;) {
            try {
                // 声明策略变量，用于决定本轮循环应该做什么操作（SELECT / CONTINUE / BUSY_WAIT 等）
                int strategy;
                
                try {
                    // ========== 步骤 1: 计算策略（判断是否有任务待执行）==========
                    // selectStrategy.calculateStrategy(selectNowSupplier, hasTasks())
                    // 根据是否有待执行的任务来决定策略：
                    // - 如果有任务，返回 CONTINUE（继续），跳过 select，直接处理任务
                    // - 如果没有任务，返回 SELECT，进行阻塞式的 select 等待 I/O 事件
                    strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks());
                    
                    // 根据策略进行不同的处理
                    switch (strategy) {
                    // ========== 策略 1: CONTINUE - 直接跳过 select，继续下一次循环 ==========
                    // 场景：任务队列中有待处理的任务，优先处理任务而不是阻塞等待 I/O
                    case SelectStrategy.CONTINUE:
                        continue;

                    // ========== 策略 2: BUSY_WAIT - 忙轮询（实际不支持，转为 SELECT）==========
                    // 注释说明 NIO 不支持忙轮询，所以降级为 SELECT
                    case SelectStrategy.BUSY_WAIT:
                        // fall-through to SELECT since the busy-wait is not supported with NIO

                    // ========== 策略 3: SELECT - 执行 select 操作等待 I/O 事件 ==========
                    case SelectStrategy.SELECT:
                        // 获取下一个定时任务的截止时间（纳秒），用于 select() 的超时参数
                        // 例如有定时任务需要在 100ms 后执行，则 select 最多等待 100ms
                        long curDeadlineNanos = nextScheduledTaskDeadlineNanos();
                        
                        // 如果返回 -1L（表示没有定时任务），则设置为 NONE（Long.MAX_VALUE）
                        // NONE 表示可以无限期阻塞，直到有 I/O 事件或外部线程唤醒
                        if (curDeadlineNanos == -1L) {
                            curDeadlineNanos = NONE; // nothing on the calendar
                        }
                        
                        // 原子性地设置下一次唤醒时间（用于外部线程调用 wakeup() 时的优化）
                        // 记录即将进入 select 的阻塞时间点，外部线程可根据此判断是否需要唤醒
                        nextWakeupNanos.set(curDeadlineNanos);
                        
                        try {
                            // ========== 关键判断：是否还有待执行的任务 ==========
                            // 双重检查：在设置唤醒时间后，再次检查是否有新任务加入
                            // 如果有任务，则不执行 select，直接去处理任务（避免阻塞）
                            if (!hasTasks()) {
                                // 没有待执行的任务，执行 select 操作
                                // 参数 curDeadlineNanos 作为超时时间，控制最长阻塞时间
                                // 返回值是本次 select 返回的就绪事件数量（可能为 0）
                                //在这里获取Selector中的select
                                strategy = select(curDeadlineNanos);
                            }
                        } finally {
                            // 无论 select 是否被执行，都要重置唤醒时间标志
                            // lazySet 是非原子的赋值，性能更高（因为单线程环境中足够安全）
                            // 重置为 AWAKE (-1L) 表示 EventLoop 已经苏醒，不需要再唤醒
                            // This update is just to help block unnecessary selector wakeups
                            // so use of lazySet is ok (no race condition)
                            nextWakeupNanos.lazySet(AWAKE);
                        }
                        
                        // 穿透 case，继续执行后续逻辑
                        // fall through
                    default:
                    }
                } catch (IOException e) {
                    // ========== 异常处理：Selector 异常（通常是 JDK bug）==========
                    // select() 抛出 IOException 通常表示 Selector 内部出现问题（epoll bug 或其他系统级故障）
                    // Netty 的解决方案：重建 Selector
                    // If we receive an IOException here its because the Selector is messed up. Let's rebuild
                    // the selector and retry. https://github.com/netty/netty/issues/8566
                    
                    // 重建 Selector：创建新的 Selector，将所有已注册的 Channel 迁移到新 Selector
                    rebuildSelector0();
                    
                    // 重置 selectCnt 为 0（清除之前的过早返回计数）
                    selectCnt = 0;
                    
                    // 处理异常：记录日志并休眠 1 秒，避免异常导致的死循环和 CPU 100%
                    handleLoopException(e);
                    
                    // 继续下一次循环（使用新的 Selector）
                    continue;
                }

                // ========== 步骤 2: 处理 select 结果 ==========
                
                // 递增 selectCnt，用于检测 select 是否频繁返回 0（JDK bug）
                // 每成功执行一次 select，就记录一次计数
                selectCnt++;
                
                // 重置取消的 keys 计数器
                // cancelledKeys 在 cancel() 方法中递增，这里每轮循环重置
                // 用于跟踪本轮循环内有多少个 key 被取消
                cancelledKeys = 0;
                
                // 重置 needsToSelectAgain 标志
                // 此标志在处理 key 时如果需要重新执行 select，会被设置为 true
                // 这里重置表示上一轮的重新 select 已处理完成
                needsToSelectAgain = false;
                
                // 读取 ioRatio 配置（原子读取，默认为 50）
                // ioRatio 控制 I/O 操作和普通任务的时间分配比例
                // 100: 100% 时间做 I/O，0% 时间做普通任务
                // 50:  50% 时间做 I/O，50% 时间做普通任务
                final int ioRatio = this.ioRatio;
                
                // 标志位：本轮循环是否执行了普通任务
                boolean ranTasks;
                
                // ========== 步骤 3: 根据 ioRatio 分配时间处理 I/O 和任务 ==========
                
                if (ioRatio == 100) {
                    // ========== 模式 1: ioRatio == 100（100% 时间做 I/O，不做任务）==========
                    // 此模式不考虑时间平衡，直接处理 I/O 再处理任务
                    try {
                        // 如果 select 返回了就绪事件（strategy > 0），则处理所有就绪的 I/O
                        if (strategy > 0) {
                            // 处理就绪的 SelectionKey：
                            // 对每个就绪的 key 执行对应的操作（read / write / connect / accept）
                            processSelectedKeys();
                        }
                    } finally {
                        // Ensure we always run tasks.
                        // 无论是否处理了 I/O，都要执行待处理的普通任务
                        // runAllTasks() 不受时间限制，会执行完所有任务再返回
                        ranTasks = runAllTasks();
                    }
                } else if (strategy > 0) {
                    // ========== 模式 2: ioRatio != 100 且 select 有返回（strategy > 0）==========
                    // 此模式根据 ioRatio 比例平衡 I/O 和任务的执行时间
                    
                    // 记录处理 I/O 的开始时间（纳秒精度）
                    final long ioStartTime = System.nanoTime();
                    try {
                        // 处理本次 select 返回的所有就绪的 I/O 事件
                        processSelectedKeys();
                    } finally {
                        // Ensure we always run tasks.
                        
                        // 计算处理 I/O 耗用的时间（纳秒）
                        final long ioTime = System.nanoTime() - ioStartTime;
                        
                        // 根据 ioRatio 比例计算应该分配给普通任务的时间
                        // 公式：taskTime = ioTime * (100 - ioRatio) / ioRatio
                        // 例如 ioRatio=50，ioTime=100ms，则 taskTime = 100 * 50 / 50 = 100ms（1:1 比例）
                        // 例如 ioRatio=75，ioTime=100ms，则 taskTime = 100 * 25 / 75 ≈ 33ms（3:1 比例）
                        ranTasks = runAllTasks(ioTime * (100 - ioRatio) / ioRatio);
                    }
                } else {
                    // ========== 模式 3: strategy == 0（select 无返回就绪事件）==========
                    // 此时说明 select 超时返回或被唤醒但无就绪事件，应该处理任务
                    // runAllTasks(0) 只执行最小数量的任务（当前任务队列中的所有任务不超过某个最小值）
                    // This will run the minimum number of tasks
                    ranTasks = runAllTasks(0);
                }

                // ========== 步骤 4: 检测并处理 select 的异常情况 ==========
                
                // 检查 select 是否过早返回（太快返回但没有处理到任务）
                // selectReturnPrematurely() 判断：
                // 1. 如果执行了任务或 strategy > 0，说明有活动，不算异常早返回
                // 2. 否则说明 select 可能空转，需要重置 selectCnt
                if (selectReturnPrematurely(selectCnt, ranTasks, strategy)) {
                    // 重置 selectCnt 为 0，开始新的计数周期
                    selectCnt = 0;
                } else if (unexpectedSelectorWakeup(selectCnt)) {
                    // ========== 检测意外的 Selector 唤醒（JDK bug）==========
                    // unexpectedSelectorWakeup() 检查：
                    // 1. 线程是否被中断（Thread.interrupted()）
                    // 2. selectCnt 是否超过阈值（表示频繁空转）
                    // 如果是，会重建 Selector 或打印警告
                    
                    // Unexpected wakeup (unusual case)
                    // 重置计数器
                    selectCnt = 0;
                }
            } catch (CancelledKeyException e) {
                // ========== 捕获 CancelledKeyException 异常 ==========
                // 当处理已被取消的 SelectionKey 时，可能抛出此异常
                // 这是一个正常的情况（不是错误），只需记录日志
                // Harmless exception - log anyway
                if (logger.isDebugEnabled()) {
                    logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector {} - JDK bug?",
                            selector, e);
                }
            } catch (Error e) {
                // ========== 捕获 Error（严重错误，直接抛出）==========
                // Error 表示 JVM 级别的严重问题（如 OutOfMemoryError），不应该捕获
                // 直接重新抛出，让调用方处理
                throw e;
            } catch (Throwable t) {
                // ========== 捕获其他异常 ==========
                // 处理所有未预期的异常，记录日志并休眠以防止死循环
                handleLoopException(t);
            } finally {
                // ========== Finally 块：优雅关闭处理 ==========
                // 无论正常执行还是异常，最后都要检查 EventLoop 是否需要关闭
                // Always handle shutdown even if the loop processing threw an exception.
                try {
                    // 检查 EventLoop 是否处于关闭状态
                    if (isShuttingDown()) {
                        // 关闭所有注册到此 EventLoop 的 Channel
                        closeAll();
                        
                        // 确认关闭完成
                        // confirmShutdown() 返回 true 表示所有 Channel 都已关闭，可以退出循环
                        if (confirmShutdown()) {
                            // 退出无限循环，EventLoop 线程终止
                            return;
                        }
                    }
                } catch (Error e) {
                    // ========== 关闭过程中的 Error（直接抛出）==========
                    throw e;
                } catch (Throwable t) {
                    // ========== 关闭过程中的异常（记录日志）==========
                    handleLoopException(t);
                }
            }
        }
    }

    // returns true if selectCnt should be reset
    private boolean selectReturnPrematurely(int selectCnt, boolean ranTasks, int strategy) {
        if (ranTasks || strategy > 0) {
            if (selectCnt > MIN_PREMATURE_SELECTOR_RETURNS && logger.isDebugEnabled()) {
                logger.debug("Selector.select() returned prematurely {} times in a row for Selector {}.",
                    selectCnt - 1, selector);
            }
            return true;
        }
        return false;
    }

    // returns true if selectCnt should be reset
    private boolean unexpectedSelectorWakeup(int selectCnt) {
        if (Thread.interrupted()) {
            // Thread was interrupted so reset selected keys and break so we not run into a busy loop.
            // As this is most likely a bug in the handler of the user or it's client library we will
            // also log it.
            //
            // See https://github.com/netty/netty/issues/2426
            if (logger.isDebugEnabled()) {
                logger.debug("Selector.select() returned prematurely because " +
                        "Thread.currentThread().interrupt() was called. Use " +
                        "NioEventLoop.shutdownGracefully() to shutdown the NioEventLoop.");
            }
            return true;
        }
        if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
                selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
            // The selector returned prematurely many times in a row.
            // Rebuild the selector to work around the problem.
            logger.warn("Selector.select() returned prematurely {} times in a row; rebuilding Selector {}.",
                    selectCnt, selector);
            rebuildSelector();
            return true;
        }
        return false;
    }

    private static void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    private void processSelectedKeys() {
        if (selectedKeys != null) {
            //在大量连接的场景下，把“遍历所有 SelectionKey”变成“只遍历真正就绪的 Key”，
            // 从而把 O(n) 降到 O(k)（k = 就绪 key 数），减少 CPU 分支与缓存失效。
            processSelectedKeysOptimized();
        } else {
            //原生方法，需要遍历所有的key
            processSelectedKeysPlain(selector.selectedKeys());
        }
    }

    @Override
    protected void cleanup() {
        try {
            selector.close();
        } catch (IOException e) {
            logger.warn("Failed to close a selector.", e);
        }
    }

    void cancel(SelectionKey key) {
        key.cancel();
        cancelledKeys ++;
        if (cancelledKeys >= CLEANUP_INTERVAL) {
            cancelledKeys = 0;
            needsToSelectAgain = true;
        }
    }

    private void processSelectedKeysPlain(Set<SelectionKey> selectedKeys) {
        // check if the set is empty and if so just return to not create garbage by
        // creating a new Iterator every time even if there is nothing to process.
        // See https://github.com/netty/netty/issues/597
        if (selectedKeys.isEmpty()) {
            return;
        }

        Iterator<SelectionKey> i = selectedKeys.iterator();
        for (;;) {
            final SelectionKey k = i.next();
            final Object a = k.attachment();
            i.remove();

            if (a instanceof AbstractNioChannel) {
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            if (!i.hasNext()) {
                break;
            }

            if (needsToSelectAgain) {
                selectAgain();
                selectedKeys = selector.selectedKeys();

                // Create the iterator again to avoid ConcurrentModificationException
                if (selectedKeys.isEmpty()) {
                    break;
                } else {
                    i = selectedKeys.iterator();
                }
            }
        }
    }

    /**
     * | 优化项                   | 实现细节                                                                                            | 效果                                       |
     * | --------------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------- |
     * | **数组替换 HashSet**      | Netty 把 `Selector.selectedKeys()` 反射替换成 **自己实现的 SelectedSelectionKeySet**（底层是 `SelectionKey[]`） | 去掉 HashSet 的哈希计算 & 链表遍历，CPU 分支 **↓50 %** |
     * | **连续内存 + 无 Iterator** | 就绪 key 直接按顺序写入数组，无需 Iterator，**无 remove 操作**                                                    | 减少 GC 根扫描、分支预测失败                         |
     * | **只遍历就绪区间**           | 记录 `size` 字段，循环 `for (int i = 0; i < size; i++)`                                                | 真正 **O(k)**，k ≈ 就绪 key 数                 |
     * | **批量唤醒优化**            | 当 EventLoop 被外部线程唤醒时，会把唤醒任务一次性处理完，避免多次 Selector wakeup                                          | 减少系统调用开销                                 |
     */
    private void processSelectedKeysOptimized() {
        for (int i = 0; i < selectedKeys.size; ++i) {
            final SelectionKey k = selectedKeys.keys[i];
            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/23631
            selectedKeys.keys[i] = null;

            final Object a = k.attachment();

            if (a instanceof AbstractNioChannel) {
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            if (needsToSelectAgain) {
                // null out entries in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                selectedKeys.reset(i + 1);

                selectAgain();
                i = -1;
            }
        }
    }

    private void processSelectedKey(SelectionKey k, AbstractNioChannel ch) {
        final AbstractNioChannel.NioUnsafe unsafe = ch.unsafe();
        if (!k.isValid()) {
            final EventLoop eventLoop;
            try {
                eventLoop = ch.eventLoop();
            } catch (Throwable ignored) {
                // If the channel implementation throws an exception because there is no event loop, we ignore this
                // because we are only trying to determine if ch is registered to this event loop and thus has authority
                // to close ch.
                return;
            }
            // Only close ch if ch is still registered to this EventLoop. ch could have deregistered from the event loop
            // and thus the SelectionKey could be cancelled as part of the deregistration process, but the channel is
            // still healthy and should not be closed.
            // See https://github.com/netty/netty/issues/5125
            if (eventLoop == this) {
                // close the channel if the key is not valid anymore
                unsafe.close(unsafe.voidPromise());
            }
            return;
        }

        /**
         * 逐行把它拆开，你就能明白 Netty 为什么要「先 OP_CONNECT，再 OP_WRITE，最后 OP_READ」，
         * 以及为什么必须 手动把 OP_CONNECT 从 interestOps 里摘掉，否则会吃光 CPU。
         * selector.select() → readyOps
         *      │
         *      ├─ OP_CONNECT → 摘掉 OP_CONNECT → finishConnect()
         *      │
         *      ├─ OP_WRITE   → flush 待写数据
         *      │
         *      └─ OP_READ    → 读/接收数据
         */
        try {
            //readyOps 是本次 epoll/select 返回的 就绪事件位图（OP_CONNECT、OP_WRITE、OP_READ、OP_ACCEPT 的组合）。
            int readyOps = k.readyOps();
            // We first need to call finishConnect() before try to trigger a read(...) or write(...) as otherwise
            // the NIO JDK channel implementation may throw a NotYetConnectedException.
            if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                // remove OP_CONNECT as otherwise Selector.select(..) will always return without blocking
                // See https://github.com/netty/netty/issues/924s
                /**
                 * 把 OP_CONNECT 从监听集合里摘掉
                 * JDK bug / 行为：只要 OP_CONNECT 还在监听位里，每次 select 都会立即返回（因为连接已就绪，位一直为 1），于是 EventLoop 空转 → CPU 100 %。
                 * Netty 用位运算 &= ~OP_CONNECT 立刻把它关掉，避免 “忙等” 死循环（issue #924 的元凶）。
                 *
                 *
                 * 与 OP_READ / OP_WRITE 的区别
                 *  --- OP_READ / OP_WRITE 是 持续型 事件：只要缓冲区可读/可写，位就会保持。
                 *  --- OP_CONNECT 是 瞬时型 事件：触发一次即完成使命，需要手动取消。
                 */
                int ops = k.interestOps();
                ops &= ~SelectionKey.OP_CONNECT;
                k.interestOps(ops);

                unsafe.finishConnect();
            }

            /**
             * 为什么 先写再读？
             * – 写操作可能把之前积压的 ByteBuf 全部 flush 掉，立刻释放内存；
             * – 如果写完仍不可写，会自动把 OP_WRITE 清掉，避免重复回调。
             */

            // Process OP_WRITE first as we may be able to write some queued buffers and so free memory.
            if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                // Call forceFlush which will also take care of clear the OP_WRITE once there is nothing left to write
               unsafe.forceFlush();
            }

            /**
             * 普通 socket：读取内核缓冲区的数据，触发 pipeline.fireChannelRead(...)
             * ServerSocket：接收新连接，触发 pipeline.fireChannelActive(...)
             * 为什么加 readyOps == 0？
             * 某些 JDK 版本在收到 TCP RST 时会出现 readyOps == 0 但仍被唤醒的空转 bug；
             * Netty 把这种情况也交给 unsafe.read() 统一兜底处理，避免 spin loop。
             */
            // Also check for readOps of 0 to workaround possible JDK bug which may otherwise lead
            // to a spin loop
            if ((readyOps & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0 || readyOps == 0) {
                unsafe.read();
            }
        } catch (CancelledKeyException ignored) {
            unsafe.close(unsafe.voidPromise());
        }
    }

    private static void processSelectedKey(SelectionKey k, NioTask<SelectableChannel> task) {
        int state = 0;
        try {
            task.channelReady(k.channel(), k);
            state = 1;
        } catch (Exception e) {
            k.cancel();
            invokeChannelUnregistered(task, k, e);
            state = 2;
        } finally {
            switch (state) {
            case 0:
                k.cancel();
                invokeChannelUnregistered(task, k, null);
                break;
            case 1:
                if (!k.isValid()) { // Cancelled by channelReady()
                    invokeChannelUnregistered(task, k, null);
                }
                break;
            default:
                 break;
            }
        }
    }

    private void closeAll() {
        selectAgain();
        Set<SelectionKey> keys = selector.keys();
        Collection<AbstractNioChannel> channels = new ArrayList<AbstractNioChannel>(keys.size());
        for (SelectionKey k: keys) {
            Object a = k.attachment();
            if (a instanceof AbstractNioChannel) {
                channels.add((AbstractNioChannel) a);
            } else {
                k.cancel();
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                invokeChannelUnregistered(task, k, null);
            }
        }

        for (AbstractNioChannel ch: channels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    private static void invokeChannelUnregistered(NioTask<SelectableChannel> task, SelectionKey k, Throwable cause) {
        try {
            task.channelUnregistered(k.channel(), cause);
        } catch (Exception e) {
            logger.warn("Unexpected exception while running NioTask.channelUnregistered()", e);
        }
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            selector.wakeup();
        }
    }

    @Override
    protected boolean beforeScheduledTaskSubmitted(long deadlineNanos) {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get();
    }

    @Override
    protected boolean afterScheduledTaskSubmitted(long deadlineNanos) {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get();
    }

    Selector unwrappedSelector() {
        return unwrappedSelector;
    }

    int selectNow() throws IOException {
        return selector.selectNow();
    }

    private int select(long deadlineNanos) throws IOException {
        if (deadlineNanos == NONE) {
            return selector.select();
        }
        // Timeout will only be 0 if deadline is within 5 microsecs
        long timeoutMillis = deadlineToDelayNanos(deadlineNanos + 995000L) / 1000000L;
        return timeoutMillis <= 0 ? selector.selectNow() : selector.select(timeoutMillis);
    }

    private void selectAgain() {
        needsToSelectAgain = false;
        try {
            selector.selectNow();
        } catch (Throwable t) {
            logger.warn("Failed to update SelectionKeys.", t);
        }
    }
}
