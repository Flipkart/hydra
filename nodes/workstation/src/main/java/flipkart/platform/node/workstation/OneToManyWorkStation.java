package flipkart.platform.node.workstation;

import java.util.Collection;
import flipkart.platform.node.jobs.OneToManyJob;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.ConcurrentQueue;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;

/**
 * A {@link flipkart.platform.node.workstation.WorkStation} that executes {@link flipkart.platform.workflow.job
 * .OneToManyJob}
 *
 * @author shashwat
 */
public class OneToManyWorkStation<I, O> extends WorkStation<I, O, OneToManyJob<I, O>>
{
    public OneToManyWorkStation(String name, int numThreads, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory, Link<O> oLink)
    {
        super(name, numThreads, queue, retryPolicy, jobFactory, oLink);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new OneToManyWorker());
    }

    private class OneToManyWorker extends WorkerBase
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
                        sendForward(o);
                    }
                    e.ack();
                }
                catch (ExecutionFailureException ex)
                {
                    if (!retryPolicy.retry(OneToManyWorkStation.this, e))
                    {
                        throw new ExecutionFailureException("No more retries after exception: " + ex.getMessage(), ex);
                    }
                }
            }
            catch (Exception ex)
            {
                job.failed(i, ex);
                e.discard(MessageCtx.DiscardAction.ENQUEUE);
            }
        }
    }

    public static <I, O> OneToManyWorkStation<I, O> create(String name,
        int numThreads, int maxAttempts,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory, Link<O> link)
    {
        return new OneToManyWorkStation<I, O>(name, numThreads, new ConcurrentQueue<I>(),
            new DefaultRetryPolicy<I>(maxAttempts), jobFactory, link);
    }
}
