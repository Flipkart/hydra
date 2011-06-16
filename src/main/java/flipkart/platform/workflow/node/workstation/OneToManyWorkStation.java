package flipkart.platform.workflow.node.workstation;

import java.util.Collection;

import flipkart.platform.workflow.job.DefaultJobFactory;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.OneToManyJob;
import flipkart.platform.workflow.link.Link;

/**
 * A {@link WorkStation} that executes {@link OneToManyJob}
 * 
 * @author shashwat
 * 
 */
public class OneToManyWorkStation<I, O> extends
        WorkStation<I, O, OneToManyJob<I, O>>
{

    public OneToManyWorkStation(String name, int numThreads, byte maxAttempts,
            JobFactory<OneToManyJob<I, O>> jobFactory, Link<O> link)
    {
        super(name, numThreads, maxAttempts, jobFactory, link);
    }

    @Override
    protected void acceptEntity(Entity<I> e)
    {
        queue.add(e);
        threadPool.execute(new OneToManyWorker());
    }

    private class OneToManyWorker extends Worker
    {
        @Override
        protected void execute(OneToManyJob<I, O> job)
        {
            final Entity<I> e = pickEntity();
            try
            {
                final Collection<O> list = job.execute(e.i);

                for (final O o : list)
                {
                    putEntity(Entity.wrap(o));
                }
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

    public static <I, O> OneToManyWorkStation<I, O> create(int numThreads,
            int maxAttempts, Class<? extends OneToManyJob<I, O>> jobClass,
            Link<O> link, String... name)
    {
        final String jobName = (name != null && name.length > 0) ? name[0]
                : jobClass.getSimpleName();
        return new OneToManyWorkStation<I, O>(jobName, numThreads,
                (byte) maxAttempts, DefaultJobFactory.create(jobClass), link);
    }
}
