package flipkart.platform.hydra.node.workstation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.common.MessageCtxBatch;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;
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
public class ManyToManyWorkStation<I, O> extends WorkStationBase<I, O, ManyToManyJob<I, O>>
{
    private final int maxJobsToGroup;
    private final long maxDelay;

    private final RefCounter jobsInQueue = new RefCounter(0);
    private final SchedulerThread schedulerThread = new SchedulerThread();

    public ManyToManyWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        RetryPolicy<I> retryPolicy, JobFactory<? extends ManyToManyJob<I, O>> jobFactory, int maxJobsToGroup,
        long maxDelayMs)
    {
        super(name,
            executorService, queue, retryPolicy, jobFactory);
        if (maxJobsToGroup <= 1 || maxDelayMs < 0)
        {
            throw new IllegalArgumentException("Illegal int arguments to: " + getClass().getSimpleName());
        }
        this.maxJobsToGroup = maxJobsToGroup;
        this.maxDelay = maxDelayMs;
        schedulerThread.start();
    }

    @Override
    protected void scheduleJob()
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
                        executeWorker(new ManyToManyWorker(newJobExecutionContext(), queue.read(jobsCommitted)));
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

    private static class ManyToManyWorker<I, O> implements Runnable
    {
        private final JobExecutionContext<I, O, ManyToManyJob<I, O>> jobExecutionContext;
        private final MessageCtxBatch<I> messageCtxBatch;

        private ManyToManyWorker(JobExecutionContext<I, O, ManyToManyJob<I, O>> jobExecutionContext,
            MessageCtxBatch<I> messageCtxBatch)
        {
            this.jobExecutionContext = jobExecutionContext;
            this.messageCtxBatch = messageCtxBatch;
        }

        @Override
        public void run()
        {
            final ManyToManyJob<I, O> job = jobExecutionContext.begin();
            if (job != null && !messageCtxBatch.isEmpty())
            {
                final List<I> jobList = new ArrayList<I>(messageCtxBatch.size());
                for (MessageCtx<I> messageCtx : messageCtxBatch)
                {
                    jobList.add(messageCtx.get());
                }

                try
                {
                    final Collection<O> outList = job.execute(jobList);
                    for (O o : outList)
                    {
                        jobExecutionContext.submitResponse(o);
                    }

                    for (MessageCtx<I> messageCtx : messageCtxBatch)
                    {
                        jobExecutionContext.succeeded(job, messageCtx);
                    }

                    messageCtxBatch.commit();
                }
                catch (Exception ex)
                {
                    for (MessageCtx<I> messageCtx : messageCtxBatch)
                    {
                        jobExecutionContext.failed(job, messageCtx, ex);
                    }
                }
                finally
                {
                    jobExecutionContext.end(job);
                }
            }
        }
    }
}
