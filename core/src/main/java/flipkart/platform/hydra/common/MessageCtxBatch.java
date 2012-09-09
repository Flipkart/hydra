package flipkart.platform.hydra.common;

import com.google.common.collect.UnmodifiableIterator;
import flipkart.platform.hydra.common.MessageCtx;

/**
 * Interface to represent a batch of messages that were dequeued from a message container. Helpful for containers that
 * provide transactions in batch message processing.
 */
public interface MessageCtxBatch<I> extends Iterable<MessageCtx<I>>
{
    /**
     * @return an iterator over {@link MessageCtx} of the dequeued messages
     */
    UnmodifiableIterator<MessageCtx<I>> iterator();

    /**
     * @return <code>true</code> if there is at least one message in this batch; <code>false</code> otherwise
     */
    boolean isEmpty();

    /**
     * @return number of messages in this batch
     */
    int size();

    /**
     * Causes the state changes recorded by individual {@link MessageCtx} returned from {@link #iterator()} to be
     * applied on the container. Can be used to implement transactions.
     *
     * If commit fails, it should throw an exception
     * TODO: error handling in case of commit failure
     */
    void commit();
}
