package flipkart.platform.workflow.node.workstation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.ManyToManyJob;
import flipkart.platform.workflow.link.Link;

/**
 * A {@link WorkStation} that executes {@link ManyToManyJob}.
 * 
 * The workstation tries to minimize the number of jobs enqueued by grouping
 * together {@link #maxJobsToGroup} jobs together.
 * 
 * @author shashwat
 * 
 * @param <I>
 * @param <O>
 */
public class ManyToManyWorkStation<I, O> extends
        LinkBasedWorkStation<I, O, ManyToManyJob<I, O>>
{
    private final int maxJobsToGroup;
    private volatile long workersInQueue = 0;
    private volatile long jobsInQueue = 0;

    public ManyToManyWorkStation(String name, int numThreads, byte maxAttempts,
            JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> link,
            int maxJobsToGroup)
    {
        super(name, numThreads, maxAttempts, jobFactory, link);
        this.maxJobsToGroup = maxJobsToGroup;
    }

    @Override
    protected void acceptEntity(Entity<I> e)
    {
        boolean addWorker = false;
        synchronized (this)
        {
            jobsInQueue += 1;
            if ((workersInQueue * maxJobsToGroup) < jobsInQueue)
            {
                workersInQueue += 1;
                addWorker = true;
            }
        }
        queue.add(e);
        if (addWorker)
        {
            threadPool.execute(new ManyToManyWorker());
        }
    }

    private class ManyToManyWorker extends Worker
    {
        @Override
        protected void execute(ManyToManyJob<I, O> job)
        {
            final int jobsCommitted;
            synchronized (ManyToManyWorkStation.this)
            {
                jobsCommitted = (int) Math.min(jobsInQueue,
                        (long) maxJobsToGroup);
                jobsInQueue -= jobsCommitted;
                workersInQueue -= 1;
            }

            if (jobsCommitted > 0)
            {
                final List<Entity<I>> entityList = new ArrayList<Entity<I>>(
                        jobsCommitted);
                final List<I> jobList = new ArrayList<I>(jobsCommitted);
                for (int count = jobsCommitted; count >= 0; --count)
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

    public static <I, O> ManyToManyWorkStation<I, O> create(String name,
            int numThreads, int maxAttempts,
            JobFactory<? extends ManyToManyJob<I, O>> jobFactory, Link<O> link,
            int maxElements) throws NoSuchMethodException
    {
        return new ManyToManyWorkStation<I, O>(name, numThreads,
                (byte) maxAttempts, jobFactory, link, maxElements);
    }
}