package flipkart.platform.workflow.utils;

import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;

/**
* User: shashwat
* Date: 29/07/12
*/
public class NoRetryPolicy<I> implements RetryPolicy<I>
{
    @Override
    public boolean retry(Node<I, ?> ioNode, MessageCtx<I> messageCtx)
    {
        return false;
    }
}
