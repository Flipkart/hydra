package flipkart.platform.node.workstation;

import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.ConcurrentQueue;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;

/**
 * A {@link flipkart.platform.node.workstation.WorkStation} that accepts and executes {@link
 * flipkart.platform.node.jobs.OneToOneJob}.
 *
 * @author shashwat
 */

public class OneToOneWorkStation<I, O> extends WorkStation<I, O, OneToOneJob<I, O>>
{
    public OneToOneWorkStation(String name, int numThreads, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> oLink)
    {
        super(name, numThreads, queue, retryPolicy, jobFactory, oLink);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new OneToOneWorker());
    }

    private class OneToOneWorker extends WorkerBase
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
                        sendForward(o);
                    }
                    e.ack();
                }
                catch (ExecutionFailureException ex)
                {
                    if (!retryPolicy.retry(OneToOneWorkStation.this, e))
                    {
                        throw new ExecutionFailureException("No more try available after exception: " +
                            ex.getMessage(), ex);
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

    public static <I, O> OneToOneWorkStation<I, O> create(String name,
        int numThreads, int maxAttempts,
        JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> link)
    {
        return new OneToOneWorkStation<I, O>(name, numThreads, new ConcurrentQueue<I>(),
            new DefaultRetryPolicy<I>(maxAttempts), jobFactory, link);
    }
}
