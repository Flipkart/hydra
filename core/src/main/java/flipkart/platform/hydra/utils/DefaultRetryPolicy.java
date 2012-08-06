package flipkart.platform.hydra.utils;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.MessageCtx;

/**
* User: shashwat
* Date: 29/07/12
*/
public class DefaultRetryPolicy<I> implements RetryPolicy<I>
{
    private final int maxAttempts;

    public DefaultRetryPolicy(int maxAttempts)
    {
        this.maxAttempts = maxAttempts;
    }

    @Override
    public boolean retry(Node<I, ?> node, MessageCtx<I> messageCtx)
    {
        if(messageCtx.getAttempt() < maxAttempts)
        {
            messageCtx.retry();
            return true;
        }
        return false;
    }
}
