package com.lmax.disruptor;

/**
 * Experimental poll-based interface for the Disruptor.
 */
public class EventPoller<T>
{
    private final DataProvider<T> dataProvider;
    private final Sequencer sequencer;
    private final Sequence sequence;
    private final Sequence gatingSequence;

    public interface Handler<T>
    {
        // 消费数据，执行自定义逻辑
        boolean onEvent(T event, long sequence, boolean endOfBatch) throws Exception;
    }

    public enum PollState
    {
        // 获取元素的状态
        PROCESSING, GATING, IDLE
    }

    public EventPoller(
        final DataProvider<T> dataProvider,
        final Sequencer sequencer,
        final Sequence sequence,
        final Sequence gatingSequence)
    {
        this.dataProvider = dataProvider;
        this.sequencer = sequencer;
        this.sequence = sequence;
        this.gatingSequence = gatingSequence;
    }

    public PollState poll(final Handler<T> eventHandler) throws Exception
    {
        // 当前元素下标
        final long currentSequence = sequence.get();
        // 下一个读取的元素
        long nextSequence = currentSequence + 1;
        // 获取已发布的元素的最大下标，即可用的元素
        final long availableSequence = sequencer.getHighestPublishedSequence(nextSequence, gatingSequence.get());
        // 如果下一个元素下标小于最大的可用元素下标
        if (nextSequence <= availableSequence)
        {
            // 是否处理下一个元素
            boolean processNextEvent;
            // 当前元素下标
            long processedSequence = currentSequence;

            try
            {
                do
                {
                    // 获取下一个元素
                    final T event = dataProvider.get(nextSequence);
                    // 处理数据，处理正常返回true
                    processNextEvent = eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    // 下一个待处理元素
                    processedSequence = nextSequence;
                    // 下一向前步进
                    nextSequence++;
                    // 如果仍有元素未处理完成
                }
                while (nextSequence <= availableSequence & processNextEvent);
            }
            finally
            {
                // 更新已处理元素下标
                sequence.set(processedSequence);
            }
            // 设置处理状态
            return PollState.PROCESSING;
        }
        // 如果当前元素下标越过了下一个元素的下标
        else if (sequencer.getCursor() >= nextSequence)
        {
            // 更改处理状态
            return PollState.GATING;
        }
        else
        {
            // 如果没有元素被发布，则更改处理状态，需要等发布之后才可以poll
            // ringBuffer中有未读取的元素，且都未被发布，等待发布后再处理
            return PollState.IDLE;
        }
    }

    public static <T> EventPoller<T> newInstance(
        final DataProvider<T> dataProvider,
        final Sequencer sequencer,
        final Sequence sequence,
        final Sequence cursorSequence,
        final Sequence... gatingSequences)
    {
        Sequence gatingSequence;
        if (gatingSequences.length == 0)
        {
            gatingSequence = cursorSequence;
        }
        else if (gatingSequences.length == 1)
        {
            gatingSequence = gatingSequences[0];
        }
        else
        {
            gatingSequence = new FixedSequenceGroup(gatingSequences);
        }

        return new EventPoller<T>(dataProvider, sequencer, sequence, gatingSequence);
    }

    public Sequence getSequence()
    {
        return sequence;
    }
}
