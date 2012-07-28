package flipkart.platform.workflow.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.MessageCtxBatch;
import flipkart.platform.workflow.queue.Queue;
import flipkart.platform.workflow.utils.RefCounter;

/**
 * User: shashwat
 * Date: 24/07/12
 */
public class ConcurrentQueue<I> implements Queue<I>
{
    private final ConcurrentLinkedQueue<MessageCtx<I>> backingQueue = new ConcurrentLinkedQueue<MessageCtx<I>>();
    private final RefCounter counter = new RefCounter(0);

    @Override
    public void enqueue(I i)
    {
        backingQueue.add(new SimpleMessageCtx(i));
        counter.offer();
    }

    @Override
    public MessageCtx<I> read()
    {
        final MessageCtx<I> messageCtx = backingQueue.poll();
        counter.take();
        return messageCtx;
    }

    @Override
    public MessageCtxBatch<I> read(int limit)
    {
        final ArrayList<MessageCtx<I>> messageCtxList = new ArrayList<MessageCtx<I>>(limit);
        for (int i = 0; i < limit; ++i)
        {
            final MessageCtx<I> messageCtx = read();
            if (messageCtx == null)
                break;
            messageCtxList.add(messageCtx);
        }
        return new SimpleMessageCtxBatch(messageCtxList);
    }

    @Override
    public int size()
    {
        return (int) counter.peek();
    }

    @Override
    public boolean isEmpty()
    {
        return counter.isZero();
    }

    /**
     * Simple message representation
     *
     * MessageCtx
     */
    protected class SimpleMessageCtx implements MessageCtx<I>
    {
        private final I i;
        private final int attempt;

        public SimpleMessageCtx(I i)
        {
            this(i, 1);
        }

        private SimpleMessageCtx(I i, int attempt)
        {
            this.i = i;
            this.attempt = attempt;
        }

        @Override
        public boolean ack()
        {
            return true;
        }

        @Override
        public int retry(int maxAttempt) throws NoMoreRetriesException
        {
            final int nextAttempt = attempt + 1;
            if(nextAttempt < maxAttempt)
            {
                backingQueue.add(new SimpleMessageCtx(i, nextAttempt));
                return nextAttempt;
            }
            throw new NoMoreRetriesException("Failed after attempts: " + attempt);
        }

        @Override
        public void discard()
        {
        }

        @Override
        public I get()
        {
            return i;
        }

        @Override
        public int getAttempt()
        {
            return attempt;
        }
    }

    /**
     * SimpleMessageCtxBatch class that implements MessageCtxBatch interface
     */
    protected class SimpleMessageCtxBatch implements MessageCtxBatch<I>
    {
        private final List<MessageCtx<I>> messageCtxes;

        public SimpleMessageCtxBatch(ArrayList<MessageCtx<I>> messageCtxes)
        {
            this.messageCtxes = Collections.unmodifiableList(messageCtxes);
        }

        @Override
        public UnmodifiableIterator<MessageCtx<I>> iterator()
        {
            return Iterators.unmodifiableIterator(messageCtxes.iterator());
        }

        @Override
        public boolean isEmpty()
        {
            return messageCtxes.isEmpty();
        }

        @Override
        public int size()
        {
            return messageCtxes.size();
        }

        @Override
        public void commit()
        {
        }
    }
}
