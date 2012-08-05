package flipkart.platform.workflow.node;

import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.utils.NoRetryPolicy;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public class BasicNode<I, O> extends AbstractNode<I, O, BasicJob<I, O>>
{
    public BasicNode(String name, HQueue<I> queue, JobFactory<? extends BasicJob<I, O>> jobFactory, Link<O> link)
    {
        super(name, MoreExecutors.sameThreadExecutor(), queue, new NoRetryPolicy<I>(),jobFactory, link);
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new WorkerBase()
        {
            @Override
            protected void execute(BasicJob<I, O> job)
            {
                final MessageCtx<I> messageCtx = queue.read();
                job.execute(messageCtx.get(), BasicNode.this, BasicNode.this.link);
                messageCtx.ack();
            }
        });
    }
}
