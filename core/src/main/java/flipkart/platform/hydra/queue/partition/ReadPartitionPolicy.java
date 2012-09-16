package flipkart.platform.hydra.queue.partition;

import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.common.MessageCtxBatch;

/**
* User: shashwat
* Date: 27/07/12
*/
public interface ReadPartitionPolicy<I>
{
    MessageCtx<I> read();

    MessageCtxBatch<I> read(int limit);
}
