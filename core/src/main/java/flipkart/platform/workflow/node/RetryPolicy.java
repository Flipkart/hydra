package flipkart.platform.workflow.node;

import com.sun.xml.internal.ws.api.message.Message;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.NoMoreRetriesException;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public interface RetryPolicy<I, O>
{
    void retry(Node<I, O> node, MessageCtx<I> messageCtx) throws NoMoreRetriesException;
}
