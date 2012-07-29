package flipkart.platform.node.workstation;

import java.util.Collection;
import flipkart.platform.node.jobs.OneToManyJob;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.NoMoreRetriesException;

/**
 * A {@link flipkart.platform.node.workstation.WorkStation} that executes {@link flipkart.platform.workflow.job
 * .OneToManyJob}
 *
 * @author shashwat
 */
public class OneToManyWorkStation<I, O> extends LinkBasedWorkStation<I, O, OneToManyJob<I, O>>
{

    public OneToManyWorkStation(String name, int numThreads, int maxAttempts,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory, Link<O> link)
    {
        super(name, numThreads, maxAttempts, jobFactory, link);
    }

    public OneToManyWorkStation(String name, int numThreads, RetryPolicy<I, O> retryPolicy,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory, Link<O> link)
    {
        super(name, numThreads, retryPolicy, jobFactory, link);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new OneToManyWorker());
    }

    private class OneToManyWorker extends Worker
    {
        @Override
        protected void execute(OneToManyJob<I, O> job)
        {
            final MessageCtx<I> e = queue.read();
            final I i = e.get();
            try
            {
                try
                {
                    final Collection<O> list = job.execute(i);

                    for (final O o : list)
                    {
                        putEntity(o);
                    }
                    e.ack();
                }
                catch (ExecutionFailureException ex)
                {
                    try
                    {
                        retryPolicy.retry(OneToManyWorkStation.this, e);
                    }
                    catch (NoMoreRetriesException fex)
                    {
                        throw new ExecutionFailureException(fex.getMessage() + ", cause: " + ex.getMessage(), ex);
                    }
                }
            }
            catch (Exception ex)
            {
                job.failed(i, ex);
                e.discard();
            }
        }
    }

    public static <I, O> OneToManyWorkStation<I, O> create(String name,
        int numThreads, int maxAttempts,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory, Link<O> link)
    {
        return new OneToManyWorkStation<I, O>(name, numThreads, maxAttempts, jobFactory, link);
    }
}
