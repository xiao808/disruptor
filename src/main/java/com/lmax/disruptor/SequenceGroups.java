/*
 * Copyright 2012 LMAX Ltd.
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

import static java.util.Arrays.copyOf;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Provides static methods for managing a {@link SequenceGroup} object.
 */
class SequenceGroups
{
    static <T> void addSequences(
        final T holder,
        final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
        final Cursored cursor,
        final Sequence... sequencesToAdd)
    {
        long cursorSequence;
        Sequence[] updatedSequences;
        Sequence[] currentSequences;

        // 使用cas更新sequence数组信息
        do
        {
            // 获取旧的sequence
            currentSequences = updater.get(holder);
            // 复制旧的sequence
            updatedSequences = copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);
            // 获取当前游标
            cursorSequence = cursor.getCursor();
            // 将新的sequence添加到数组中
            int index = currentSequences.length;
            for (Sequence sequence : sequencesToAdd)
            {
                // 保存当前游标信息
                sequence.set(cursorSequence);
                updatedSequences[index++] = sequence;
            }
        }
        while (!updater.compareAndSet(holder, currentSequences, updatedSequences));
        // 更新游标信息，cas操作
        cursorSequence = cursor.getCursor();
        for (Sequence sequence : sequencesToAdd)
        {
            sequence.set(cursorSequence);
        }
    }

    static <T> boolean removeSequence(
        final T holder,
        final AtomicReferenceFieldUpdater<T, Sequence[]> sequenceUpdater,
        final Sequence sequence)
    {
        int numToRemove;
        Sequence[] oldSequences;
        Sequence[] newSequences;
        // 使用cas更新barrier sequence数组
        do
        {
            // 获取原有的sequence数组
            oldSequences = sequenceUpdater.get(holder);
            // 对比两个数组，找出公有的元素数目
            numToRemove = countMatching(oldSequences, sequence);

            if (0 == numToRemove)
            {
                break;
            }

            final int oldSize = oldSequences.length;
            newSequences = new Sequence[oldSize - numToRemove];
            // 拷贝数组，跳过sequence，保持原有的数组顺序
            for (int i = 0, pos = 0; i < oldSize; i++)
            {
                final Sequence testSequence = oldSequences[i];
                if (sequence != testSequence)
                {
                    newSequences[pos++] = testSequence;
                }
            }
        }
        while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));
        // 移除的元素个数
        return numToRemove != 0;
    }

    private static <T> int countMatching(T[] values, final T toMatch)
    {
        int numToRemove = 0;
        for (T value : values)
        {
            if (value == toMatch) // Specifically uses identity
            {
                numToRemove++;
            }
        }
        return numToRemove;
    }
}
