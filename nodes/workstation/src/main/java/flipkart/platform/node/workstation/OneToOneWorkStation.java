package flipkart.platform.node.workstation;

import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.NoMoreRetriesException;

/**
 * A {@link flipkart.platform.node.workstation.WorkStation} that accepts and executes {@link
 * flipkart.platform.node.jobs.OneToOneJob}.
 *
 * @author shashwat
 */

public class OneToOneWorkStation<I, O> extends LinkBasedWorkStation<I, O, OneToOneJob<I, O>>
{
    public OneToOneWorkStation(final String name, int numThreads, int maxAttempts,
        final JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> link)
    {
        super(name, numThreads, maxAttempts, jobFactory, link);
    }

    public OneToOneWorkStation(final String name, int numThreads, RetryPolicy<I, O> retryPolicy,
        final JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> link)
    {
        super(name, numThreads, retryPolicy, jobFactory, link);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new OneToOneWorker());
    }

    //@Override
    //protected void scheduleWorker()
    //{
    //    threadPool.execute(new OneToOneWorker());
    //}

    private class OneToOneWorker extends Worker
    {
        @Override
        protected void execute(OneToOneJob<I, O> job)
        {
            final MessageCtx<I> e = queue.read();
            final I i = e.get();
            try
            {
                try
                {
                    final O o = job.execute(i);

                    if (o != null)
                    {
                        putEntity(o);
                    }
                    e.ack();
                }
                catch (ExecutionFailureException ex)
                {
                    try
                    {
                        retryPolicy.retry(OneToOneWorkStation.this, e);
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

    public static <I, O> OneToOneWorkStation<I, O> create(String name,
        int numThreads, int maxAttempts,
        JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> link)
    {
        return new OneToOneWorkStation<I, O>(name, numThreads, maxAttempts, jobFactory, link);
    }
}
