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

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

import com.lmax.disruptor.util.Util;


/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> * Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.</p>
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
    private static final Unsafe UNSAFE = Util.getUnsafe();
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    // 位置标记，已分配的元素位置会被打上标记
    private final int[] availableBuffer;
    // 用于下标取模，获取元素位置
    private final int indexMask;
    // 下标偏移量
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        // 使用bufferSize以2为底的对数
        indexShift = Util.log2(bufferSize);
        // 初始化已分配元素标记
        initialiseAvailableBuffer();
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
    {
        // 分配空间后的下标
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        // 上次缓存的元素下标
        long cachedGatingSequence = gatingSequenceCache.get();
        // 由于数组是环形，分配指定空间大小之后
        // 1: 新的下标回到了数组的头部，且超过了上一次的读取地址，则如果分配了空间之后，有元素会被覆盖，即没有足够的空间
        // 2: 上一次的缓存地址大于下一个元素的地址，可能使用claim跳过了某些元素
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            // 获取当前元素之前所有未读元素的最小下标
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            // 缓存该未读元素位置
            gatingSequenceCache.set(minSequence);
            // 如果要分配的下标大于最小的未读元素下标，返回false，无足够空间
            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        // 获取下一个元素的位置
        return next(1);
    }

    /**
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n)
    {
        if (n < 1 || n > bufferSize)
        {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        long current;
        long next;

        do
        {
            // 获取当前元素下标
            current = cursor.get();
            // 待分配的元素下标
            next = current + n;
            // 待分配的元素
            long wrapPoint = next - bufferSize;
            // 上次读取的缓存元素
            long cachedGatingSequence = gatingSequenceCache.get();
            // 由于数组是环形，分配指定空间大小之后
            // 1: 新的下标回到了数组的头部，且超过了上一次的读取地址，则如果分配了空间之后，有元素会被覆盖，即没有足够的空间
            // 2: 上一次的缓存地址大于下一个元素的地址，可能使用claim跳过了某些元素
            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
            {
                // 当前元素之前的所有元素的最小下标
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);
                // 如果分配之后的元素位置中存在未读元素，则等待执行完成
                if (wrapPoint > gatingSequence)
                {
                    LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
                    continue;
                }
                // 缓存当前的位置，尽快读取
                gatingSequenceCache.set(gatingSequence);
            }
            else if (cursor.compareAndSet(current, next))
            {
                // 如果下标设置成功，则返回，否则循环执行以上逻辑
                break;
            }
        }
        while (true);
        // 返回元素下标
        return next;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        // 尝试获取下一个元素
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do
        {
            // 获取当前下标
            current = cursor.get();
            // 待获取的元素下标
            next = current + n;
            // 如果没有足够的空间，抛出异常
            if (!hasAvailableCapacity(gatingSequences, n, current))
            {
                throw InsufficientCapacityException.INSTANCE;
            }
            // 之后获取成功，否则循环执行以上逻辑
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        // 已读取的元素下标的最小位置
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        // 已写入的元素位置
        long produced = cursor.get();
        // bufferSize减去未读元素的最大连续集合的大小，即为剩余的元素空间
        return getBufferSize() - (produced - consumed);
    }

    private void initialiseAvailableBuffer()
    {
        for (int i = availableBuffer.length - 1; i != 0; i--)
        {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        // 将当前元素位置设置为可用
        setAvailable(sequence);
        // 执行等待通知
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * The below methods work on the availableBuffer flag.
     * <p>
     * The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     * <p>
     * --  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    private void setAvailable(final long sequence)
    {
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    private void setAvailableBufferValue(int index, int flag)
    {
        // 计算内存地址
        long bufferAddress = (index * SCALE) + BASE;
        // 分配内存空间
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        // 当前元素下标
        int index = calculateIndex(sequence);
        // 元素位置可用标识
        int flag = calculateAvailabilityFlag(sequence);
        // 该元素的内存地址
        long bufferAddress = (index * SCALE) + BASE;
        // 查看当前元素的标识是否为可用
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        // 在指定范围内获取已发布的元素位置
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            if (!isAvailable(sequence))
            {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    private int calculateAvailabilityFlag(final long sequence)
    {
        // 计算元素可用标识
        return (int) (sequence >>> indexShift);
    }

    private int calculateIndex(final long sequence)
    {
        // 计算数组下标
        return ((int) sequence) & indexMask;
    }
}
