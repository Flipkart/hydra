package flipkart.platform.workflow.utils;

import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;

/**
* User: shashwat
* Date: 29/07/12
*/
public class DefaultRetryPolicy<I> implements RetryPolicy<I>
{
    private final int maxRetries;

    public DefaultRetryPolicy(int maxRetries)
    {
        this.maxRetries = maxRetries;
    }

    @Override
    public boolean retry(Node<I, ?> node, MessageCtx<I> messageCtx)
    {
        if(messageCtx.getAttempt() < maxRetries)
        {
            messageCtx.retry();
            return true;
        }
        return false;
    }
}
