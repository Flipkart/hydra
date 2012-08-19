package flipkart.platform.hydra.node.builder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.ConcurrentQueue;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.utils.DefaultRetryPolicy;
import flipkart.platform.hydra.utils.HydraThreadFactory;
import flipkart.platform.hydra.utils.NoRetryPolicy;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public abstract class AbstractNodeBuilder<I, O> implements NodeBuilder<I,O>
{
    protected String name = "";
    protected RetryPolicy<I> retryPolicy = new NoRetryPolicy<I>();
    protected ExecutorService executorService = MoreExecutors.sameThreadExecutor();
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
    public NodeBuilder<I, O> withExecutor(ExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withThreadExecutor(int numThreads)
    {
        return withExecutor(Executors.newFixedThreadPool(numThreads, new HydraThreadFactory(name)));
    }

    @Override
    public NodeBuilder<I, O> withThreadExecutor(int numThreads, ThreadFactory threadFactory)
    {
        return withExecutor(Executors.newFixedThreadPool(numThreads, threadFactory));
    }

    @Override
    public NodeBuilder<I, O> withQueue(HQueue<I> hQueue)
    {
        this.queue = hQueue;
        return this;
    }
}
