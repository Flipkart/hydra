package flipkart.platform.hydra.utils;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.MessageCtx;

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
