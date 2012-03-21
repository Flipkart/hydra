package flipkart.platform.workflow.node.workstation;

import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.ManyToManyJob;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.utils.RefCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link WorkStation} that executes {@link ManyToManyJob}.
 * <p/>
 * The workstation tries to minimize the number of jobs enqueued by grouping
 * together {@link #maxJobsToGroup} jobs together.
 *
 * @param <I>
 * @param <O>
 * @author shashwat
 */
public class ManyToManyWorkStation<I, O> extends
    LinkBasedWorkStation<I, O, ManyToManyJob<I, O>>
{
    private final int maxJobsToGroup;
    private final long maxDelay;

    private final RefCounter jobsInQueue = new RefCounter(0);
    private final SchedulerThread schedulerThread = new SchedulerThread();

    public ManyToManyWorkStation(String name, int numThreads, int maxAttempts,
        JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> link,
        int maxJobsToGroup)
    {
        this(name, numThreads, maxAttempts, jobFactory, link, maxJobsToGroup, 0, TimeUnit.MILLISECONDS);
    }

    public ManyToManyWorkStation(String name, int numThreads, int maxAttempts,
        JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> link,
        int maxJobsToGroup, long maxDelay, TimeUnit unit)
    {
        super(name, numThreads, maxAttempts, jobFactory, link);
        if (maxJobsToGroup <= 1 || maxDelay < 0)
        {
            throw new IllegalArgumentException("Illegal int arguments to: " + getClass().getSimpleName());
        }

        this.maxJobsToGroup = maxJobsToGroup;
        this.maxDelay = unit.toMillis(maxDelay);
        schedulerThread.start();
    }

    @Override
    protected void acceptEntity(Entity<I> e)
    {
        final long currentJobsCount = jobsInQueue.offer();
        queue.add(e);
        if (currentJobsCount == maxJobsToGroup)
        {
            schedulerThread.interrupt();
        }
    }

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        schedulerThread.shutdown();
        schedulerThread.join();
        super.shutdown(awaitTermination);
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
                    threadPool.execute(new ManyToManyWorker(jobsCommitted));
                } while (jobsInQueue.peek() > maxJobsToGroup || interrupted()); // loop while we are being interrupted
            }
        }

        public void shutdown()
        {
            shutdown = true;
            interrupt();
        }
    }

    private class ManyToManyWorker extends Worker
    {
        private final int jobsCommitted;

        private ManyToManyWorker(int jobsCommitted)
        {
            this.jobsCommitted = jobsCommitted;
        }

        @Override
        protected void execute(ManyToManyJob<I, O> job)
        {
            //waitingWorkers.incrementAndGet();
            //final int jobsCommitted = (int) jobsInQueue.take(maxJobsToGroup);

            if (jobsCommitted > 0)
            {
                final List<Entity<I>> entityList = new ArrayList<Entity<I>>(jobsCommitted);
                final List<I> jobList = new ArrayList<I>(jobsCommitted);
                for (int count = 0; count < jobsCommitted; ++count)
                {
                    final Entity<I> e = pickEntity();
                    if (e == null)
                    {
                        break;
                    }
                    entityList.add(e);
                    jobList.add(e.i);
                }

                try
                {
                    final Collection<O> outList = job.execute(jobList);
                    for (O o : outList)
                    {
                        putEntity(Entity.wrap(o));
                    }
                }
                catch (ExecutionFailureException ex)
                {
                    for (Entity<I> e : entityList)
                    {
                        try
                        {
                            putBack(e);
                        }
                        catch (NoMoreRetriesException fex)
                        {
                            job.failed(
                                e.i,
                                new ExecutionFailureException(fex
                                    .getMessage()
                                    + ", cause: "
                                    + ex.getMessage(), ex));
                        }
                    }
                }
                catch (Exception ex)
                {
                    for (Entity<I> e : entityList)
                    {
                        job.failed(e.i, ex);
                    }
                }

            }
        }
    }

    public static <I, O> ManyToManyWorkStation<I, O> create(String name, int numThreads, int maxAttempts,
        JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> link, int maxElements)
    {
        return new ManyToManyWorkStation<I, O>(name, numThreads, maxAttempts, jobFactory, link, maxElements);
    }

    public static <I, O> ManyToManyWorkStation<I, O> create(String name, int numThreads, int maxAttempts,
        JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> link, int maxElements, long maxDelay,
        TimeUnit unit)
    {
        return new ManyToManyWorkStation<I, O>(name, numThreads, (byte) maxAttempts, jobFactory, link, maxElements,
            maxDelay, unit);
    }
}
