package flipkart.platform.workflow.queue.partition;

import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.MessageCtxBatch;

/**
* User: shashwat
* Date: 27/07/12
*/
public interface ReadPartitionPolicy<I>
{
    MessageCtx<I> read();

    MessageCtxBatch<I> read(int limit);
}
