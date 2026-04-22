/*
 * Copyright 2014 The Netty Project
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
package io.netty.channel.epoll;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.epoll.AbstractEpollChannel.AbstractEpollUnsafe;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.IovArray;
import io.netty.util.IntSupplier;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;

/**
 * {@link EventLoop} which uses epoll under the covers. Only works on Linux!
 */
/**
 * EpollEventLoop 是专门针对 Linux epoll 机制的事件循环实现。
 * 
 * 设计理由：
 * 1. 继承 SingleThreadEventLoop，保证线程安全和事件处理的串行性
 * 2. 利用 Linux epoll 提供高性能、低延迟的 I/O 多路复用机制
 * 3. 优化网络事件处理，支持高并发、高吞吐量场景
 * 4. 提供细粒度的事件监听和处理能力
 */
public class EpollEventLoop extends SingleThreadEventLoop {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(EpollEventLoop.class);
    private static final long EPOLL_WAIT_MILLIS_THRESHOLD =
            SystemPropertyUtil.getLong("io.netty.channel.epoll.epollWaitThreshold", 10);

    static {
        // Ensure JNI is initialized by the time this class is loaded by this time!
        // We use unix-common methods in this class which are backed by JNI methods.
        Epoll.ensureAvailability();
    }

    // epollFd: 用于 Linux epoll 实例的文件描述符
    // 设计理由：创建 epoll 实例，用于高效监听多个文件描述符上的 I/O 事件
    private FileDescriptor epollFd;

    // eventFd: 用于异步事件通知的文件描述符
    // 设计理由：支持在不同线程间低开销地发送事件通知，实现高效的事件唤醒机制
    private FileDescriptor eventFd;

    // timerFd: 定时器文件描述符
    // 设计理由：提供精确的定时器功能，允许在 epoll 中直接监听定时事件，减少额外系统调用
    private FileDescriptor timerFd;

    // channels: 维护文件描述符到 EpollChannel 的映射
    // 设计理由：提供快速的 fd 到 Channel 查找，支持高效的事件分发和管理
    // 初始大小 4096，预留足够空间以减少扩容开销
    private final IntObjectMap<AbstractEpollChannel> channels = new IntObjectHashMap<AbstractEpollChannel>(4096);

    // allowGrowing: 是否允许 events 数组动态增长
    // 设计理由：在高负载场景下，自动扩展 events 数组以适应突发性能需求
    private final boolean allowGrowing;

    // events: 存储 epoll 返回的就绪事件数组
    // 设计理由：减少内存分配开销，重复使用同一个数组，提高性能
    private final EpollEventArray events;

    // These are initialized on first use
    private IovArray iovArray;
    private NativeDatagramPacketArray datagramPacketArray;

    private final SelectStrategy selectStrategy;
    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            return epollWaitNow();
        }
    };

    private static final long AWAKE = -1L;
    private static final long NONE = Long.MAX_VALUE;

    // nextWakeupNanos is:
    //    AWAKE            when EL is awake
    //    NONE             when EL is waiting with no wakeup scheduled
    //    other value T    when EL is waiting with wakeup scheduled at time T
    private final AtomicLong nextWakeupNanos = new AtomicLong(AWAKE);
    private boolean pendingWakeup;
    private volatile int ioRatio = 50;

    // See https://man7.org/linux/man-pages/man2/timerfd_create.2.html.
    private static final long MAX_SCHEDULED_TIMERFD_NS = 999999999;

    EpollEventLoop(EventLoopGroup parent, Executor executor, int maxEvents,
                   SelectStrategy strategy, RejectedExecutionHandler rejectedExecutionHandler,
                   EventLoopTaskQueueFactory taskQueueFactory, EventLoopTaskQueueFactory tailTaskQueueFactory) {
        super(parent, executor, false, newTaskQueue(taskQueueFactory), newTaskQueue(tailTaskQueueFactory),
                rejectedExecutionHandler);
        selectStrategy = ObjectUtil.checkNotNull(strategy, "strategy");
        if (maxEvents == 0) {
            allowGrowing = true;
            events = new EpollEventArray(4096);
        } else {
            allowGrowing = false;
            events = new EpollEventArray(maxEvents);
        }
        openFileDescriptors();
    }

    /**
     * This method is intended for use by a process checkpoint/restore
     * integration, such as OpenJDK CRaC.
     */
    @UnstableApi
    public void openFileDescriptors() {
        boolean success = false;
        FileDescriptor epollFd = null;
        FileDescriptor eventFd = null;
        FileDescriptor timerFd = null;
        try {
            this.epollFd = epollFd = Native.newEpollCreate();
            this.eventFd = eventFd = Native.newEventFd();
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call eventfd_read(...).
                Native.epollCtlAdd(epollFd.intValue(), eventFd.intValue(), Native.EPOLLIN | Native.EPOLLET);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to add eventFd filedescriptor to epoll", e);
            }
            this.timerFd = timerFd = Native.newTimerFd();
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call read(...).
                Native.epollCtlAdd(epollFd.intValue(), timerFd.intValue(), Native.EPOLLIN | Native.EPOLLET);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to add timerFd filedescriptor to epoll", e);
            }
            success = true;
        } finally {
            if (!success) {
                closeFileDescriptor(epollFd);
                closeFileDescriptor(eventFd);
                closeFileDescriptor(timerFd);
            }
        }
    }

    private static void closeFileDescriptor(FileDescriptor fd) {
        if (fd != null) {
            try {
                fd.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private static Queue<Runnable> newTaskQueue(
            EventLoopTaskQueueFactory queueFactory) {
        if (queueFactory == null) {
            return newTaskQueue0(DEFAULT_MAX_PENDING_TASKS);
        }
        return queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS);
    }

    /**
     * Return a cleared {@link IovArray} that can be used for writes in this {@link EventLoop}.
     */
    IovArray cleanIovArray() {
        if (iovArray == null) {
            iovArray = new IovArray();
        } else {
            iovArray.clear();
        }
        return iovArray;
    }

    /**
     * Return a cleared {@link NativeDatagramPacketArray} that can be used for writes in this {@link EventLoop}.
     */
    NativeDatagramPacketArray cleanDatagramPacketArray() {
        if (datagramPacketArray == null) {
            datagramPacketArray = new NativeDatagramPacketArray();
        } else {
            datagramPacketArray.clear();
        }
        return datagramPacketArray;
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            // write to the evfd which will then wake-up epoll_wait(...)
            Native.eventFdWrite(eventFd.intValue(), 1L);
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

    /**
     * Register the given epoll with this {@link EventLoop}.
     */
    void add(AbstractEpollChannel ch) throws IOException {
        assert inEventLoop();
        int fd = ch.socket.intValue();
        Native.epollCtlAdd(epollFd.intValue(), fd, ch.flags);
        AbstractEpollChannel old = channels.put(fd, ch);

        // We either expect to have no Channel in the map with the same FD or that the FD of the old Channel is already
        // closed.
        assert old == null || !old.isOpen();
    }

    /**
     * The flags of the given epoll was modified so update the registration
     */
    void modify(AbstractEpollChannel ch) throws IOException {
        assert inEventLoop();
        Native.epollCtlMod(epollFd.intValue(), ch.socket.intValue(), ch.flags);
    }

    /**
     * Deregister the given epoll from this {@link EventLoop}.
     */
    void remove(AbstractEpollChannel ch) throws IOException {
        assert inEventLoop();
        int fd = ch.socket.intValue();

        AbstractEpollChannel old = channels.remove(fd);
        if (old != null && old != ch) {
            // The Channel mapping was already replaced due FD reuse, put back the stored Channel.
            channels.put(fd, old);

            // If we found another Channel in the map that is mapped to the same FD the given Channel MUST be closed.
            assert !ch.isOpen();
        } else if (ch.isOpen()) {
            // Remove the epoll. This is only needed if it's still open as otherwise it will be automatically
            // removed once the file-descriptor is closed.
            Native.epollCtlDel(epollFd.intValue(), fd);
        }
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
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    public int getIoRatio() {
        return ioRatio;
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop.  The default value is
     * {@code 50}, which means the event loop will try to spend the same amount of time for I/O as for non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)");
        }
        this.ioRatio = ioRatio;
    }

    @Override
    public int registeredChannels() {
        return channels.size();
    }

    @Override
    public Iterator<Channel> registeredChannelsIterator() {
        assert inEventLoop();
        IntObjectMap<AbstractEpollChannel> ch = channels;
        if (ch.isEmpty()) {
            return ChannelsReadOnlyIterator.empty();
        }
        return new ChannelsReadOnlyIterator<AbstractEpollChannel>(ch.values());
    }

    private long epollWait(long deadlineNanos) throws IOException {
        if (deadlineNanos == NONE) {
            return Native.epollWait(epollFd, events, timerFd,
                    Integer.MAX_VALUE, 0, EPOLL_WAIT_MILLIS_THRESHOLD); // disarm timer
        }
        long totalDelay = deadlineToDelayNanos(deadlineNanos);
        int delaySeconds = (int) min(totalDelay / 1000000000L, Integer.MAX_VALUE);
        int delayNanos = (int) min(totalDelay - delaySeconds * 1000000000L, MAX_SCHEDULED_TIMERFD_NS);
        return Native.epollWait(epollFd, events, timerFd, delaySeconds, delayNanos, EPOLL_WAIT_MILLIS_THRESHOLD);
    }

    private int epollWaitNoTimerChange() throws IOException {
        return Native.epollWait(epollFd, events, false);
    }

    private int epollWaitNow() throws IOException {
        return Native.epollWait(epollFd, events, true);
    }

    private int epollBusyWait() throws IOException {
        return Native.epollBusyWait(epollFd, events);
    }

    private int epollWaitTimeboxed() throws IOException {
        // Wait with 1 second "safeguard" timeout
        return Native.epollWait(epollFd, events, 1000);
    }

    /**
     * EpollEventLoop 的核心事件循环方法，实现复杂的事件处理和任务调度逻辑。
     * 
     * 设计理由：
     * 1. 提供高性能、低延迟的事件驱动模型
     * 2. 灵活的任务调度策略
     * 3. 平衡 I/O 事件处理和其他任务执行
     * 4. 支持动态唤醒和超时控制
     * 5. 异常处理和优雅关闭
     */
    @Override
    protected void run() {
        // prevDeadlineNanos: 记录上一次定时器截止时间，避免不必要的定时器重新设置
        long prevDeadlineNanos = NONE;
        
        // 无限循环，持续处理事件和任务
        for (;;) {
            try {
                // 根据当前状态选择事件处理策略
                // 设计理由：提供灵活的事件处理模式，如立即返回、忙等待、阻塞等待
                int strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks());
                switch (strategy) {
                    case SelectStrategy.CONTINUE:
                        // 无需处理，继续下一轮循环
                        continue;

                    case SelectStrategy.BUSY_WAIT:
                        // 高频繁事件时采用忙等待，减少系统调用开销
                        strategy = epollBusyWait();
                        break;

                    case SelectStrategy.SELECT:
                        // 阻塞等待事件，处理复杂的定时和唤醒逻辑
                        if (pendingWakeup) {
                            // 有挂起的唤醒事件，限时等待
                            strategy = epollWaitTimeboxed();
                            if (strategy != 0) {
                                break;
                            }
                            // 超时处理：可能发生异常的系统调用
                            logger.warn("Missed eventfd write (not seen after > 1 second)");
                            pendingWakeup = false;
                            if (hasTasks()) {
                                break;
                            }
                        }

                        // 计算下一个定时任务的截止时间
                        long curDeadlineNanos = nextScheduledTaskDeadlineNanos();
                        if (curDeadlineNanos == -1L) {
                            curDeadlineNanos = NONE; // 无定时任务
                        }
                        nextWakeupNanos.set(curDeadlineNanos);
                        
                        try {
                            if (!hasTasks()) {
                                if (curDeadlineNanos == prevDeadlineNanos) {
                                    // 无需调整定时器
                                    strategy = epollWaitNoTimerChange();
                                } else {
                                    // 重新设置或取消定时器
                                    long result = epollWait(curDeadlineNanos);
                                    strategy = Native.epollReady(result);
                                    prevDeadlineNanos = Native.epollTimerWasUsed(result) ? curDeadlineNanos : NONE;
                                }
                            }
                        } finally {
                            // 处理可能的异步唤醒
                            if (nextWakeupNanos.get() == AWAKE || nextWakeupNanos.getAndSet(AWAKE) == AWAKE) {
                                pendingWakeup = true;
                            }
                        }
                        // fallthrough
                    default:
                }

                // I/O 事件处理比例控制
                // 设计理由：平衡 I/O 和非 I/O 任务的执行时间
                final int ioRatio = this.ioRatio;
                if (ioRatio == 100) {
                    // 100% 用于 I/O，性能最大化
                    try {
                        if (strategy > 0 && processReady(events, strategy)) {
                            prevDeadlineNanos = NONE;
                        }
                    } finally {
                        // 确保执行所有任务
                        runAllTasks();
                    }
                } else if (strategy > 0) {
                    // 动态分配 I/O 和任务执行时间
                    final long ioStartTime = System.nanoTime();
                    try {
                        if (processReady(events, strategy)) {
                            prevDeadlineNanos = NONE;
                        }
                    } finally {
                        // 根据 I/O 耗时计算任务执行时间
                        final long ioTime = System.nanoTime() - ioStartTime;
                        runAllTasks(ioTime * (100 - ioRatio) / ioRatio);
                    }
                } else {
                    // 无 I/O 事件时执行最小数量任务
                    runAllTasks(0);
                }

                // 动态扩展事件数组
                // 设计理由：适应高负载场景，避免事件丢失
                if (allowGrowing && strategy == events.length()) {
                    events.increase();
                }
            } catch (Error e) {
                throw e;
            } catch (Throwable t) {
                // 处理非致命异常，避免事件循环中断
                handleLoopException(t);
            } finally {
                // 处理关闭流程，确保资源正确释放
                try {
                    if (isShuttingDown()) {
                        closeAll();
                        if (confirmShutdown()) {
                            break;
                        }
                    }
                } catch (Error e) {
                    throw e;
                } catch (Throwable t) {
                    handleLoopException(t);
                }
            }
        }
    }

    /**
     * Visible only for testing!
     */
    void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    private void closeAll() {
        // Using the intermediate collection to prevent ConcurrentModificationException.
        // In the `close()` method, the channel is deleted from `channels` map.
        AbstractEpollChannel[] localChannels = channels.values().toArray(new AbstractEpollChannel[0]);

        for (AbstractEpollChannel ch: localChannels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    // Returns true if a timerFd event was encountered
    private boolean processReady(EpollEventArray events, int ready) {
        boolean timerFired = false;
        for (int i = 0; i < ready; i ++) {
            final int fd = events.fd(i);
            if (fd == eventFd.intValue()) {
                pendingWakeup = false;
            } else if (fd == timerFd.intValue()) {
                timerFired = true;
            } else {
                final long ev = events.events(i);

                AbstractEpollChannel ch = channels.get(fd);
                if (ch != null) {
                    // Don't change the ordering of processing EPOLLOUT | EPOLLRDHUP / EPOLLIN if you're not 100%
                    // sure about it!
                    // Re-ordering can easily introduce bugs and bad side-effects, as we found out painfully in the
                    // past.
                    AbstractEpollUnsafe unsafe = (AbstractEpollUnsafe) ch.unsafe();

                    // First check for EPOLLOUT as we may need to fail the connect ChannelPromise before try
                    // to read from the file descriptor.
                    // See https://github.com/netty/netty/issues/3785
                    //
                    // It is possible for an EPOLLOUT or EPOLLERR to be generated when a connection is refused.
                    // In either case epollOutReady() will do the correct thing (finish connecting, or fail
                    // the connection).
                    // See https://github.com/netty/netty/issues/3848
                    if ((ev & (Native.EPOLLERR | Native.EPOLLOUT)) != 0) {
                        // Force flush of data as the epoll is writable again
                        unsafe.epollOutReady();
                    }

                    // Check EPOLLIN before EPOLLRDHUP to ensure all data is read before shutting down the input.
                    // See https://github.com/netty/netty/issues/4317.
                    //
                    // If EPOLLIN or EPOLLERR was received and the channel is still open call epollInReady(). This will
                    // try to read from the underlying file descriptor and so notify the user about the error.
                    if ((ev & (Native.EPOLLERR | Native.EPOLLIN)) != 0) {
                        // The Channel is still open and there is something to read. Do it now.
                        unsafe.epollInReady();
                    }

                    // Check if EPOLLRDHUP was set, this will notify us for connection-reset in which case
                    // we may close the channel directly or try to read more data depending on the state of the
                    // Channel and als depending on the AbstractEpollChannel subtype.
                    if ((ev & Native.EPOLLRDHUP) != 0) {
                        unsafe.epollRdHupReady();
                    }
                } else {
                    // We received an event for an fd which we not use anymore. Remove it from the epoll_event set.
                    try {
                        Native.epollCtlDel(epollFd.intValue(), fd);
                    } catch (IOException ignore) {
                        // This can happen but is nothing we need to worry about as we only try to delete
                        // the fd from the epoll set as we not found it in our mappings. So this call to
                        // epollCtlDel(...) is just to ensure we cleanup stuff and so may fail if it was
                        // deleted before or the file descriptor was closed before.
                    }
                }
            }
        }
        return timerFired;
    }

    @Override
    protected void cleanup() {
        try {
            closeFileDescriptors();
        } finally {
            // release native memory
            if (iovArray != null) {
                iovArray.release();
                iovArray = null;
            }
            if (datagramPacketArray != null) {
                datagramPacketArray.release();
                datagramPacketArray = null;
            }
            events.free();
        }
    }

    /**
     * This method is intended for use by process checkpoint/restore
     * integration, such as OpenJDK CRaC.
     * It's up to the caller to ensure that there is no concurrent use
     * of the FDs while these are closed, e.g. by blocking the executor.
     */
    @UnstableApi
    public void closeFileDescriptors() {
        // Ensure any in-flight wakeup writes have been performed prior to closing eventFd.
        while (pendingWakeup) {
            try {
                int count = epollWaitTimeboxed();
                if (count == 0) {
                    // We timed-out so assume that the write we're expecting isn't coming
                    break;
                }
                for (int i = 0; i < count; i++) {
                    if (events.fd(i) == eventFd.intValue()) {
                        pendingWakeup = false;
                        break;
                    }
                }
            } catch (IOException ignore) {
                // ignore
            }
        }
        try {
            eventFd.close();
        } catch (IOException e) {
            logger.warn("Failed to close the event fd.", e);
        }
        try {
            timerFd.close();
        } catch (IOException e) {
            logger.warn("Failed to close the timer fd.", e);
        }

        try {
            epollFd.close();
        } catch (IOException e) {
            logger.warn("Failed to close the epoll fd.", e);
        }
    }
}
