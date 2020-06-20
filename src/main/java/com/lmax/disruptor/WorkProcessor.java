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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>A {@link WorkProcessor} wraps a single {@link WorkHandler}, effectively consuming the sequence
 * and ensuring appropriate barriers.</p>
 *
 * <p>Generally, this will be used as part of a {@link WorkerPool}.</p>
 *
 * @param <T> event implementation storing the details for the work to processed.
 */
public final class WorkProcessor<T>
    implements EventProcessor
{
    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);
    // sequence初始值
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    // sequence存放数组
    private final RingBuffer<T> ringBuffer;
    // 当前的同步器
    private final SequenceBarrier sequenceBarrier;
    // consumers
    private final WorkHandler<? super T> workHandler;
    // 异常处理
    private final ExceptionHandler<? super T> exceptionHandler;
    //
    private final Sequence workSequence;
    // 放弃操作，将sequence设置为long的最大值，即最后被执行
    private final EventReleaser eventReleaser = new EventReleaser()
    {
        @Override
        public void release()
        {
            sequence.set(Long.MAX_VALUE);
        }
    };
    // 超时处理
    private final TimeoutHandler timeoutHandler;

    /**
     * Construct a {@link WorkProcessor}.
     *
     * @param ringBuffer       to which events are published.
     * @param sequenceBarrier  on which it is waiting.
     * @param workHandler      is the delegate to which events are dispatched.
     * @param exceptionHandler to be called back when an error occurs
     * @param workSequence     from which to claim the next event to be worked on.  It should always be initialised
     *                         as {@link Sequencer#INITIAL_CURSOR_VALUE}
     */
    public WorkProcessor(
        final RingBuffer<T> ringBuffer,
        final SequenceBarrier sequenceBarrier,
        final WorkHandler<? super T> workHandler,
        final ExceptionHandler<? super T> exceptionHandler,
        final Sequence workSequence)
    {
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.workHandler = workHandler;
        this.exceptionHandler = exceptionHandler;
        this.workSequence = workSequence;
        // 如果consumer设置了事件出让规则
        if (this.workHandler instanceof EventReleaseAware)
        {
            ((EventReleaseAware) this.workHandler).setEventReleaser(eventReleaser);
        }

        timeoutHandler = (workHandler instanceof TimeoutHandler) ? (TimeoutHandler) workHandler : null;
    }

    @Override
    public Sequence getSequence()
    {
        return sequence;
    }

    @Override
    public void halt()
    {
        // 关闭当前processor，并执行通知
        running.set(false);
        sequenceBarrier.alert();
    }

    /**
     * remove workProcessor dynamic without message lost
     */
    public void haltLater()
    {
        // 设置为关闭状态，不可再添加任务，任务执行完成后进行关闭
        running.set(false);
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }

    /**
     * It is ok to have another thread re-run this method after a halt().
     *
     * @throws IllegalStateException if this processor is already running
     */
    @Override
    public void run()
    {
        if (!running.compareAndSet(false, true))
        {
            throw new IllegalStateException("Thread is already running");
        }
        // 清楚告警信息
        sequenceBarrier.clearAlert();
        // 通知等待的线程启动
        notifyStart();

        boolean processedSequence = true;
        long cachedAvailableSequence = Long.MIN_VALUE;
        long nextSequence = sequence.get();
        T event = null;
        // loop循环
        while (true)
        {
            try
            {
                // if previous sequence was processed - fetch the next sequence and set
                // that we have successfully processed the previous sequence
                // typically, this will be true
                // this prevents the sequence getting too far forward if an exception
                // is thrown from the WorkHandler
                // 如果前置的事件正常处理，将继续执行一下逻辑，如果前置事件处理异常，将不向下执行，防止向前跳过太多事件
                if (processedSequence)
                {
                    // 检查运行状态
                    if (!running.get())
                    {
                        // 不在运行状态，执行通知
                        sequenceBarrier.alert();
                        // 退出执行
                        sequenceBarrier.checkAlert();
                    }
                    // 不向前计算sequence
                    processedSequence = false;
                    do
                    {
                        // 寻找下一个需要执行的事件
                        nextSequence = workSequence.get() + 1L;
                        sequence.set(nextSequence - 1L);
                    }
                    while (!workSequence.compareAndSet(nextSequence - 1L, nextSequence));
                }
                // 如果nextSequence已被跳过，则执行nextSequence
                if (cachedAvailableSequence >= nextSequence)
                {
                    // 获取当前事件
                    event = ringBuffer.get(nextSequence);
                    // 执行处理逻辑
                    workHandler.onEvent(event);
                    // 继续向前计算sequence
                    processedSequence = true;
                }
                else
                {
                    // 等待nextSequence执行
                    cachedAvailableSequence = sequenceBarrier.waitFor(nextSequence);
                }
            }
            catch (final TimeoutException e)
            {
                // 超时通知
                notifyTimeout(sequence.get());
            }
            catch (final AlertException ex)
            {
                // 当前processor已关闭，跳出事件处理
                if (!running.get())
                {
                    break;
                }
            }
            catch (final Throwable ex)
            {
                // 执行异常，执行异常处理逻辑，当前任务标记为执行完成
                // handle, mark as processed, unless the exception handler threw an exception
                exceptionHandler.handleEventException(ex, nextSequence, event);
                processedSequence = true;
            }
        }
        // 执行完成或者被关闭，发出通知
        notifyShutdown();

        running.set(false);
    }

    private void notifyTimeout(final long availableSequence)
    {
        try
        {
            if (timeoutHandler != null)
            {
                timeoutHandler.onTimeout(availableSequence);
            }
        }
        catch (Throwable e)
        {
            exceptionHandler.handleEventException(e, availableSequence, null);
        }
    }

    private void notifyStart()
    {
        // 如果handler需要启动通知
        if (workHandler instanceof LifecycleAware)
        {
            try
            {
                ((LifecycleAware) workHandler).onStart();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    private void notifyShutdown()
    {
        // 如果handler需要停止通知
        if (workHandler instanceof LifecycleAware)
        {
            try
            {
                ((LifecycleAware) workHandler).onShutdown();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }
}
