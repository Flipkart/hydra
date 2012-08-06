package flipkart.platform.hydra.node.builder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import flipkart.platform.hydra.link.Link;
import flipkart.platform.hydra.link.Selector;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;

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

    NodeBuilder<I, O> withMaxAttempts(int maxAttempts);

    NodeBuilder<I, O> withExecutor(ExecutorService executorService);

    NodeBuilder<I, O> withThreadExecutor(int numThreads);

    NodeBuilder<I, O> withThreadExecutor(int numThreads, ThreadFactory threadFactory);

    NodeBuilder<I, O> withQueue(HQueue<I> queue);

    Node<I, O> build();
}
