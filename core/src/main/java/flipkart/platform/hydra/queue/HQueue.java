package flipkart.platform.hydra.queue;

import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.common.MessageCtxBatch;

/**
 * A message queue abstraction over in-memory concurrent queue or distributed queues.
 *
 * @param <I>
 *     message type
 *     User: shashwat
 *     Date: 23/07/12
 */
public interface HQueue<I>
{
    /**
     * Enqueue a message into this queue
     *
     * @param i
     *     message that needs to be added
     */
    void enqueue(I i);

    /**
     * Read and remove one message from the queue
     *
     * @return {@link MessageCtx} if queue was not empty, <code>null</code>, if there are no messages to read.
     */
    MessageCtx<I> read();

    /**
     * Batch read one or more message from the queue. The number of messages read may be less than the limit provided
     * in case the queue {@link #size()} was less than the limit
     *
     * @param limit
     *     maximum number of messages that needs to be read from the queue
     * @return {@link MessageCtxBatch} if one or messages were read; <code>null</code> otherwise
     */
    MessageCtxBatch<I> read(int limit);

    /**
     * @return queue size
     */
    int size();

    /**
     * @return <code>true</code> if the message queue is not empty. It also means that {@link #size()} > 0.
     */
    boolean isEmpty();
}
