package flipkart.platform.hydra.node;

import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.hydra.job.BasicJob;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.link.Link;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.queue.MessageCtx;
import flipkart.platform.hydra.utils.NoRetryPolicy;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public class BasicNode<I, O> extends AbstractNode<I, O, BasicJob<I, O>>
{
    public BasicNode(String name, HQueue<I> queue, JobFactory<? extends BasicJob<I, O>> jobFactory)
    {
        super(name, MoreExecutors.sameThreadExecutor(), queue, new NoRetryPolicy<I>(), jobFactory);
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
                job.execute(messageCtx.get(), this);
                messageCtx.ack();
            }
        });
    }
}
