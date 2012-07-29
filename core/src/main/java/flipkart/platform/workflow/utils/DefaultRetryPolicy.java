package flipkart.platform.workflow.utils;

import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.NoMoreRetriesException;

/**
* User: shashwat
* Date: 29/07/12
*/
public class DefaultRetryPolicy<I, O> implements RetryPolicy<I, O>
{
    private final int maxRetries;

    public DefaultRetryPolicy(int maxRetries)
    {
        this.maxRetries = maxRetries;
    }

    @Override
    public void retry(Node<I, O> node, MessageCtx<I> messageCtx) throws NoMoreRetriesException
    {
        messageCtx.retry(maxRetries);
    }
}
