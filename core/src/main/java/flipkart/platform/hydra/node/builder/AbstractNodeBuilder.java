package flipkart.platform.hydra.node.builder;

import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.ConcurrentQueue;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.utils.DefaultRetryPolicy;
import flipkart.platform.hydra.utils.NoRetryPolicy;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public abstract class AbstractNodeBuilder<I, O> implements NodeBuilder<I,O>
{
    protected String name = "";
    protected RetryPolicy<I> retryPolicy = new NoRetryPolicy<I>();
    protected HQueue<I> queue = new ConcurrentQueue<I>();

    public AbstractNodeBuilder(String name)
    {
        this.name = name;
    }

    @Override
    public NodeBuilder<I, O> withName(String name)
    {
        this.name = name;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withRetry(RetryPolicy<I> retryPolicy)
    {
        this.retryPolicy = retryPolicy;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withMaxAttempts(int maxAttempts)
    {
        return withRetry(new DefaultRetryPolicy<I>(maxAttempts));
    }

    @Override
    public NodeBuilder<I, O> withQueue(HQueue<I> hQueue)
    {
        this.queue = hQueue;
        return this;
    }
}
