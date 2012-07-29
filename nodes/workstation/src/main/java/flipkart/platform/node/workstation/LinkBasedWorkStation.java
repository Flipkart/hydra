package flipkart.platform.node.workstation;

import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;

abstract class LinkBasedWorkStation<I, O, J extends Job<I>> extends WorkStation<I, O, J>
{
    protected final Link<O> link;

    public LinkBasedWorkStation(String name, int numThreads, int maxAttempts, final JobFactory<? extends J> jobFactory,
        Link<O> link)
    {
        this(name, numThreads, new DefaultRetryPolicy<I, O>(maxAttempts), jobFactory, link);
    }

    public LinkBasedWorkStation(String name, int numThreads, RetryPolicy<I, O> retryPolicy,
        final JobFactory<? extends J> jobFactory, Link<O> link)
    {
        super(name, numThreads, retryPolicy, jobFactory);
        this.link = link;
    }

    public void append(Node<O, ?> node)
    {
        link.append(node);
    }

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        super.shutdown(awaitTermination);
        link.sendShutdown(awaitTermination);
    }

    protected void putEntity(O o)
    {
        link.forward(o);
    }
}
