package flipkart.platform.workflow.node.workstation;

import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.OneToOneJob;
import flipkart.platform.workflow.link.Link;

/**
 * A {@link WorkStation} that accepts and executes {@link OneToOneJob}.
 * 
 * @author shashwat
 * 
 */

public class OneToOneWorkStation<I, O> extends
        LinkBasedWorkStation<I, O, OneToOneJob<I, O>>
{

    public OneToOneWorkStation(final String name, int numThreads,
            final byte maxAttempts,
            final JobFactory<? extends OneToOneJob<I, O>> jobFactory,
            Link<O> link)
    {
        super(name, numThreads, maxAttempts, jobFactory, link);
    }

    @Override
    protected void acceptEntity(Entity<I> e)
    {
        queue.add(e);
        threadPool.execute(new OneToOneWorker());
    }

    private class OneToOneWorker extends Worker
    {
        @Override
        protected void execute(OneToOneJob<I, O> job)
        {
            final Entity<I> e = pickEntity();
            try
            {
                final O o = job.execute(e.i);

                putEntity(Entity.wrap(o));
            }
            catch (ExecutionFailureException ex)
            {
                try
                {
                    putBack(e);
                }
                catch (NoMoreRetriesException fex)
                {
                    job.failed(e.i,
                            new ExecutionFailureException(fex.getMessage()
                                    + ", cause: " + ex.getMessage(), ex));
                }
            }
            catch (Exception ex)
            {
                job.failed(e.i, ex);
            }
        }

    }

    public static <I, O> OneToOneWorkStation<I, O> create(String name,
            int numThreads, int maxAttempts,
            JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> link)
    {
        return new OneToOneWorkStation<I, O>(name, numThreads,
                (byte) maxAttempts, jobFactory, link);
    }
}