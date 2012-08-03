package flipkart.platform.node.builder;

import java.util.concurrent.ExecutorService;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.link.Selector;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.HQueue;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public interface NodeBuilder<I, O>
{
    NodeBuilder<I, O> withName(String name);

    NodeBuilder<I, O> withLink(Link<O> link);

    NodeBuilder<I, O> withSelector(Selector<O> selector);

    NodeBuilder<I, O> withRetry(RetryPolicy<I> retryPolicy);

    NodeBuilder<I, O> withMaxRetries(int maxRetries);

    NodeBuilder<I, O> withExecutor(ExecutorService executorService);

    NodeBuilder<I, O> withQueue(HQueue<I> queue);
    
    Node<I, O> build();
}
