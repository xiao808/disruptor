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

import com.lmax.disruptor.util.Util;
// 缓存填充
abstract class SingleProducerSequencerPad extends AbstractSequencer
{
    protected long p1, p2, p3, p4, p5, p6, p7;

    SingleProducerSequencerPad(int bufferSize, WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad
{
    SingleProducerSequencerFields(int bufferSize, WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * Set to -1 as sequence starting point
     * 下一个可用的元素位置
     */
    long nextValue = Sequence.INITIAL_VALUE;
    /**
     * 上一次执行hasAvailableCapacity、next记录的位置
     */
    long cachedValue = Sequence.INITIAL_VALUE;
}

/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Not safe for use from multiple threads as it does not implement any barriers.</p>
 *
 * <p>* Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#publish(long)} is made.</p>
 */

public final class SingleProducerSequencer extends SingleProducerSequencerFields
{
    // 缓存填充，尾部填充
    protected long p1, p2, p3, p4, p5, p6, p7;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public SingleProducerSequencer(int bufferSize, WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(int requiredCapacity)
    {
        return hasAvailableCapacity(requiredCapacity, false);
    }

    private boolean hasAvailableCapacity(int requiredCapacity, boolean doStore)
    {
        // 下一个元素的地址
        long nextValue = this.nextValue;
        // 增加指定的元素数量之后的sequence下标
        long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
        // 前一次缓存的sequence元素
        long cachedGatingSequence = this.cachedValue;
        // 由于数组是环形，分配指定空间大小之后
        // 1: 新的下标回到了数组的头部，且超过了上一次的读取地址，则如果分配了空间之后，有元素会被覆盖，即没有足够的空间
        // 2: 上一次的缓存地址大于下一个元素的地址，可能使用claim跳过了某些元素
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            if (doStore)
            {
                // 设置当前有标位置
                cursor.setVolatile(nextValue);  // StoreLoad fence
            }
            // 在下一元素向后寻找未读的元素
            long minSequence = Util.getMinimumSequence(gatingSequences, nextValue);
            // 缓存元素
            this.cachedValue = minSequence;
            // 如果分配后的下标大于最小的未读元素下标，则分配元素后数据会被覆盖，即无可用空间
            if (wrapPoint > minSequence)
            {
                return false;
            }
        }
        // 有足够的空间分配元素
        return true;
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        // 依次寻找后面一个元素
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
        // 下一个元素
        long nextValue = this.nextValue;
        // 下一个元素后的第n个元素
        long nextSequence = nextValue + n;
        // 要获取的元素位置
        long wrapPoint = nextSequence - bufferSize;
        // 上一次读取元素的位置
        long cachedGatingSequence = this.cachedValue;
        // 由于数组是环形，分配指定空间大小之后
        // 1: 新的下标回到了数组的头部，且超过了上一次的读取地址，则如果分配了空间之后，有元素会被覆盖，即没有足够的空间
        // 2: 上一次的缓存地址大于下一个元素的地址，可能使用claim跳过了某些元素
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            // 设置当前游标
            cursor.setVolatile(nextValue);  // StoreLoad fence
            // 在将要读取的元素之前有未读元素，则等待执行完成
            long minSequence;
            while (wrapPoint > (minSequence = Util.getMinimumSequence(gatingSequences, nextValue)))
            {
                // 如果有未读元素，则等待读取完毕
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin?
            }
            // 记录当前位置
            this.cachedValue = minSequence;
        }
        // 记录可用的元素位置并返回
        this.nextValue = nextSequence;

        return nextSequence;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
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
        // 如果没有可用的空间，则抛出异常
        if (!hasAvailableCapacity(n, true))
        {
            throw InsufficientCapacityException.INSTANCE;
        }
        // 待获取的元素位置
        long nextSequence = this.nextValue += n;
        // 直接返回
        return nextSequence;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        // 下一未读元素的位置
        long nextValue = this.nextValue;
        // 已读取的的元素的最小位置
        long consumed = Util.getMinimumSequence(gatingSequences, nextValue);
        // 已写入的元素位置
        long produced = nextValue;
        // 用总的bufferSize减去未读元素的大小，剩余的空间几位可用空间
        return getBufferSize() - (produced - consumed);
    }

    /**
     *
     * 从下一个指定的位置读取元素
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        this.nextValue = sequence;
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(long sequence)
    {
        cursor.set(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi)
    {
        publish(hi);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        return sequence <= cursor.get();
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        return availableSequence;
    }
}
