package flipkart.platform.hydra.node.workstation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.link.Link;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.queue.MessageCtx;
import flipkart.platform.hydra.queue.MessageCtxBatch;
import flipkart.platform.hydra.utils.RefCounter;

/**
 * A {@link AbstractNode} that executes {@link ManyToManyJob}.
 * <p/>
 * The workstation tries to minimize the number of jobs enqueued by grouping
 * together {@link #maxJobsToGroup} jobs together.
 *
 * @param <I>
 * @param <O>
 * @author shashwat
 */
public class ManyToManyWorkStation<I, O> extends AbstractNode<I, O, ManyToManyJob<I, O>>
{
    private final int maxJobsToGroup;
    private final long maxDelay;

    private final RefCounter jobsInQueue = new RefCounter(0);
    private final SchedulerThread schedulerThread = new SchedulerThread();

    public ManyToManyWorkStation(String name, ExecutorService executorService, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> oLink, int maxJobsToGroup, long maxDelayMs)
    {
        super(name, executorService, queue, retryPolicy, jobFactory, oLink);
        if (maxJobsToGroup <= 1 || maxDelayMs < 0)
        {
            throw new IllegalArgumentException("Illegal int arguments to: " + getClass().getSimpleName());
        }
        this.maxJobsToGroup = maxJobsToGroup;
        this.maxDelay = maxDelayMs;
        schedulerThread.start();
    }

    @Override
    protected void scheduleWorker()
    {
        final long currentJobsCount = jobsInQueue.offer();
        if (currentJobsCount == maxJobsToGroup)
        {
            schedulerThread.interrupt();
        }
    }

    @Override
    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        schedulerThread.shutdown();
        schedulerThread.join();
        super.shutdownResources(awaitTermination);
    }

    protected class SchedulerThread extends Thread
    {
        private volatile boolean shutdown = false;

        @Override
        public void run()
        {
            while (!shutdown)
            {
                try
                {
                    Thread.sleep(maxDelay);
                }
                catch (InterruptedException e)
                {
                    //
                }

                do
                {
                    final int jobsCommitted = (int) jobsInQueue.take(maxJobsToGroup);
                    if (jobsCommitted == 0)
                        break;
                    try
                    {
                        executeWorker(new ManyToManyWorker(jobsCommitted));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                } while (jobsInQueue.peek() > maxJobsToGroup || interrupted()); // loop while we are being interrupted
            }
        }

        public void shutdown()
        {
            shutdown = true;
            interrupt();
        }
    }

    private class ManyToManyWorker extends WorkerBase
    {
        private final int jobsCommitted;

        private ManyToManyWorker(int jobsCommitted)
        {
            this.jobsCommitted = jobsCommitted;
        }

        @Override
        protected void execute(ManyToManyJob<I, O> job)
        {
            if (jobsCommitted > 0)
            {
                final MessageCtxBatch<I> messageCtxBatch = queue.read(jobsCommitted);

                final List<I> jobList = new ArrayList<I>(jobsCommitted);
                for (MessageCtx<I> messageCtx : messageCtxBatch)
                {
                    jobList.add(messageCtx.get());
                }

                try
                {
                    final Collection<O> outList = job.execute(jobList);
                    for (O o : outList)
                    {
                        sendForward(o);
                    }

                    for (MessageCtx<I> messageCtx : messageCtxBatch)
                    {
                        messageCtx.ack();
                    }
                    messageCtxBatch.commit();
                }
                catch (ExecutionFailureException ex)
                {
                    for (MessageCtx<I> messageCtx : messageCtxBatch)
                    {
                        retryMessage(job, messageCtx, ex);
                    }
                }
                catch (Exception ex)
                {
                    for (MessageCtx<I> messageCtx : messageCtxBatch)
                    {
                        discardMessage(job, messageCtx, ex);
                    }
                }

            }
        }
    }
}
