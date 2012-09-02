package flipkart.platform.hydra.node.workstation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import flipkart.platform.hydra.job.Job;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;

/**
 * User: shashwat
 * Date: 31/08/12
 */
public abstract class WorkStationBase<I, O, J extends Job<I>> extends AbstractNode<I, O, J>
{
    private final ExecutorService executorService;

    protected WorkStationBase(String name, ExecutorService executorService, HQueue<I> queue,
        RetryPolicy<I> retryPolicy, JobFactory<? extends J> jobFactory)
    {
        super(name, queue, retryPolicy, jobFactory);
        this.executorService = executorService;
    }

    protected void executeWorker(Runnable runnable)
    {
        executorService.execute(runnable);
    }

    @Override
    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        executorService.shutdown();
        while (awaitTermination
            && !executorService.awaitTermination(10, TimeUnit.MILLISECONDS))
            ;

        super.shutdownResources(awaitTermination);
    }
}
