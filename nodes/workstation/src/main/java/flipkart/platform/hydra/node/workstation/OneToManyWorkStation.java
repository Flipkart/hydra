package flipkart.platform.hydra.node.workstation;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.OneToManyJob;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.queue.MessageCtx;

/**
 * A {@link AbstractNode} that executes {@link OneToManyJob}
 *
 * @author shashwat
 */
public class OneToManyWorkStation<I, O> extends AbstractNode<I, O, OneToManyJob<I, O>>
{
    public OneToManyWorkStation(String name, ExecutorService executorService, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory)
    {
        super(name, executorService, queue, retryPolicy, jobFactory);
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
            final MessageCtx<I> messageCtx = queue.read();
            final I i = messageCtx.get();
            try
            {
                try
                {
                    final Collection<O> list = job.execute(i);
                    ackMessage(messageCtx, list);
                }
                catch (ExecutionFailureException ex)
                {
                    retryMessage(job, messageCtx, ex);
                }
            }
            catch (Exception ex)
            {
                discardMessage(job, messageCtx, ex);
            }
        }

        private void ackMessage(MessageCtx<I> messageCtx, Collection<O> list)
        {
            for (final O o : list)
            {
                sendForward(o);
            }
            messageCtx.ack();
        }
    }
}
