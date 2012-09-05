package flipkart.platform.hydra.node.workstation;

import java.util.Queue;
import java.util.concurrent.*;
import flipkart.platform.hydra.job.Job;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.ConcurrentQueue;
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
        try
        {
            executorService.execute(runnable);
        }
        catch (RejectedExecutionException e)
        {
            handleRejectedWorker(runnable, executorService);
        }
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

    public void handleRejectedWorker(Runnable runnable, ExecutorService executorService)
    {
        // If executor cannot run it, then let's better execute it in this thread only. This way,
        // it will block the other requests from overwhelming this pipeline
        // Consequences being that there will be spurious jobs created for threads that are not part of this node
        // (to know why this will happen, see AbstractNode.NodeJobExecutionContext.begin()
        // but then it is a small trade-of in an overwhelmed system
        // TODO: log that execution is happening in some other thread
        runnable.run();
    }
}
