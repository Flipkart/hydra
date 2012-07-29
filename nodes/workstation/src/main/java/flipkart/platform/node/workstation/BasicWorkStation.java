package flipkart.platform.node.workstation;

import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.utils.NoRetryPolicy;

/**
 * A {@link flipkart.platform.node.workstation.WorkStation} that accepts and executes {@link
 * flipkart.platform.workflow.job.BasicJob}.
 *
 * @author shashwat
 */

public class BasicWorkStation<I, O> extends LinkBasedWorkStation<I, O, BasicJob<I, O>>
{
    public BasicWorkStation(String name, int numThreads, JobFactory<? extends BasicJob<I, O>> jobFactory,
        Link<O> oLink)
    {
        super(name, numThreads, new NoRetryPolicy<I, O>(), jobFactory, oLink);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new BasicWorker());
    }

    public static <I, O> BasicWorkStation<I, O> create(String name, int numThreads,
        JobFactory<? extends BasicJob<I, O>> jobFactory, Link<O> link)
    {
        return new BasicWorkStation<I, O>(name, numThreads, jobFactory, link);
    }

    private class BasicWorker extends Worker
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
                messageCtx.discard();
            }
        }

    }

}
