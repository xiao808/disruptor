/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import com.lmax.disruptor.util.ThreadHints;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Variation of the {@link BlockingWaitStrategy} that attempts to elide conditional wake-ups when
 * the lock is uncontended.  Shows performance improvements on microbenchmarks.  However this
 * wait strategy should be considered experimental as I have not full proved the correctness of
 * the lock elision code.
 */
public final class LiteBlockingWaitStrategy implements WaitStrategy
{
    /**
     * 同步锁
     */
    private final Object mutex = new Object();
    /**
     * 是否需要进行signal唤醒
     */
    private final AtomicBoolean signalNeeded = new AtomicBoolean(false);

    @Override
    public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
        throws AlertException, InterruptedException
    {
        long availableSequence;
        if (cursorSequence.get() < sequence)
        {
            // 如果小于sequence index，需要进行等待
            synchronized (mutex)
            {
                // 重复检测状态直到当前index大于指定的sequence，即指定的任务完成
                do
                {
                    // 转换提醒状态
                    signalNeeded.getAndSet(true);
                    // 再次检测，如果大于指定的sequence index，则跳过
                    if (cursorSequence.get() >= sequence)
                    {
                        break;
                    }
                    // 发出警告信息
                    barrier.checkAlert();
                    // 进入等待状态
                    mutex.wait();
                }
                while (cursorSequence.get() < sequence);
            }
        }
        // 依次检测sequence，直到当前sequence完成
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            // 发出告警信息
            barrier.checkAlert();
            // 线程自旋
            ThreadHints.onSpinWait();
        }
        // 返回可用的sequence
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        // 转换通知状态
        if (signalNeeded.getAndSet(false))
        {
            synchronized (mutex)
            {
                // 唤醒所有的等待线程
                mutex.notifyAll();
            }
        }
    }

    @Override
    public String toString()
    {
        return "LiteBlockingWaitStrategy{" +
            "mutex=" + mutex +
            ", signalNeeded=" + signalNeeded +
            '}';
    }
}
