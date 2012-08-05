package flipkart.platform.node.workstation;

import java.util.concurrent.ExecutorService;
import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AbstractNode;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.utils.NoRetryPolicy;

/**
 * A {@link AbstractNode} that accepts and executes {@link flipkart.platform.workflow.job.BasicJob}.
 *
 * @author shashwat
 */

public class BasicWorkStation<I, O> extends AbstractNode<I, O, BasicJob<I, O>>
{
    public BasicWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        JobFactory<? extends BasicJob<I, O>> basicJobJobFactory, Link<O> oLink)
    {
        super(name, executorService, queue, new NoRetryPolicy<I>(), basicJobJobFactory, oLink);
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
                discardMessage(job, messageCtx, ex);
            }
        }

    }

}
