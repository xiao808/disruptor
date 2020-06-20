package com.lmax.disruptor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lmax.disruptor.util.Util.awaitNanos;

/**
 * Variation of the {@link TimeoutBlockingWaitStrategy} that attempts to elide conditional wake-ups
 * when the lock is uncontended.
 */
public class LiteTimeoutBlockingWaitStrategy implements WaitStrategy
{
    // 同步锁
    private final Object mutex = new Object();
    // 唤醒状态
    private final AtomicBoolean signalNeeded = new AtomicBoolean(false);
    // 超时时间
    private final long timeoutInNanos;

    public LiteTimeoutBlockingWaitStrategy(final long timeout, final TimeUnit units)
    {
        timeoutInNanos = units.toNanos(timeout);
    }

    @Override
    public long waitFor(
        final long sequence,
        final Sequence cursorSequence,
        final Sequence dependentSequence,
        final SequenceBarrier barrier)
        throws AlertException, InterruptedException, TimeoutException
    {
        long nanos = timeoutInNanos;

        long availableSequence;
        // 如果当前任务未执行
        if (cursorSequence.get() < sequence)
        {
            // 锁定
            synchronized (mutex)
            {
                // 循环等待，直至任务完成或者超时
                while (cursorSequence.get() < sequence)
                {
                    // 设置通知状态为需要唤醒
                    signalNeeded.getAndSet(true);
                    // 执行告警
                    barrier.checkAlert();
                    // 自旋等待，如果超时抛出异常
                    nanos = awaitNanos(mutex, nanos);
                    if (nanos <= 0)
                    {
                        throw TimeoutException.INSTANCE;
                    }
                }
            }
        }
        // 任务执行完成之后，顺序查找下一个可用的sequence
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            // 发出告警
            barrier.checkAlert();
        }
        // 返回可用的sequence
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        if (signalNeeded.getAndSet(false))
        {
            synchronized (mutex)
            {
                mutex.notifyAll();
            }
        }
    }

    @Override
    public String toString()
    {
        return "LiteTimeoutBlockingWaitStrategy{" +
            "mutex=" + mutex +
            ", signalNeeded=" + signalNeeded +
            ", timeoutInNanos=" + timeoutInNanos +
            '}';
    }
}
