package flipkart.platform.workflow.utils;

import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.NoMoreRetriesException;

/**
* User: shashwat
* Date: 29/07/12
*/
public class NoRetryPolicy<I, O> implements RetryPolicy<I,O>
{
    @Override
    public void retry(Node<I, O> ioNode, MessageCtx<I> messageCtx) throws NoMoreRetriesException
    {
        throw new NoMoreRetriesException("No more retries available");
    }
}
