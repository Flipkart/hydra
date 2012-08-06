package flipkart.platform.hydra.queue;

import com.google.common.collect.UnmodifiableIterator;

/**
 * User: shashwat
 * Date: 23/07/12
 */
public interface MessageCtxBatch<I> extends Iterable<MessageCtx<I>>
{
    UnmodifiableIterator<MessageCtx<I>> iterator();

    boolean isEmpty();

    int size();

    void commit();
}
