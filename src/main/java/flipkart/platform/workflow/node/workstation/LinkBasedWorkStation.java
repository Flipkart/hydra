package flipkart.platform.workflow.node.workstation;

import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.Node;

abstract class LinkBasedWorkStation<I, O, J extends Job<I>> extends
        WorkStation<I, O, J>
{
    protected final Link<O> link;

    public LinkBasedWorkStation(String name, int numThreads, int maxAttempts,
            JobFactory<? extends J> jobFactory, Link<O> link)
    {
        super(name, numThreads, maxAttempts, jobFactory);
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

    protected void putEntity(Entity<O> e)
    {
        link.forward(e.i);
    }
}
