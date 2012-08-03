package flipkart.platform.node.builder;

import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.workflow.link.DefaultLink;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.link.Selector;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.ConcurrentQueue;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;
import flipkart.platform.workflow.utils.NoRetryPolicy;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public abstract class AbstractNodeBuilder<I, O> implements WSNodeBuilder<I, O>
{
    protected String name = "";
    protected Link<O> link = new DefaultLink<O>();
    protected RetryPolicy<I> retryPolicy = new NoRetryPolicy<I>();
    protected ExecutorService executorService = MoreExecutors.sameThreadExecutor();
    protected int numThreads = 3;
    protected HQueue<I> queue = new ConcurrentQueue<I>();

    @Override
    public NodeBuilder<I, O> withName(String name)
    {
        this.name = name;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withLink(Link<O> oLink)
    {
        this.link = link;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withSelector(Selector<O> selector)
    {
        return withLink(new DefaultLink<O>(selector));
    }

    @Override
    public NodeBuilder<I, O> withRetry(RetryPolicy<I> retryPolicy)
    {
        this.retryPolicy = retryPolicy;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withMaxRetries(int maxRetries)
    {
        return withRetry(new DefaultRetryPolicy<I>(maxRetries));
    }

    @Override
    public NodeBuilder<I, O> withExecutor(ExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    @Override
    public WSNodeBuilder<I, O> withNumThreads(int numThreads)
    {
        this.numThreads = numThreads;
        return this;
    }

    @Override
    public NodeBuilder<I, O> withQueue(HQueue<I> hQueue)
    {
        this.queue = hQueue;
        return this;
    }
}
