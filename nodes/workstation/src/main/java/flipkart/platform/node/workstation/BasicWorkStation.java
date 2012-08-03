package flipkart.platform.node.workstation;

import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.queue.ConcurrentQueue;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.utils.NoRetryPolicy;

/**
 * A {@link flipkart.platform.node.workstation.WorkStation} that accepts and executes {@link
 * flipkart.platform.workflow.job.BasicJob}.
 *
 * @author shashwat
 */

public class BasicWorkStation<I, O> extends WorkStation<I, O, BasicJob<I, O>>
{
    public BasicWorkStation(String name, int numThreads, HQueue<I> queue,
        JobFactory<? extends BasicJob<I, O>> basicJobJobFactory, Link<O> oLink)
    {
        super(name, numThreads, queue, new NoRetryPolicy<I>(), basicJobJobFactory, oLink);
    }

    public static <I, O> BasicWorkStation<I, O> create(String name, int numThreads,
        JobFactory<? extends BasicJob<I, O>> jobFactory, Link<O> link)
    {
        return new BasicWorkStation<I, O>(name, numThreads, new ConcurrentQueue<I>(), jobFactory, link);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new BasicWorker());
    }

    private class BasicWorker extends WorkerBase
    {
        @Override
        protected void execute(BasicJob<I, O> job)
        {
            final MessageCtx<I> messageCtx = queue.read();
            final I i = messageCtx.get();
            try
            {
                job.execute(i, BasicWorkStation.this, link);
                messageCtx.ack();
            }
            catch (Exception ex)
            {
                job.failed(i, ex);
                messageCtx.discard(MessageCtx.DiscardAction.ENQUEUE);
            }
        }

    }

}
