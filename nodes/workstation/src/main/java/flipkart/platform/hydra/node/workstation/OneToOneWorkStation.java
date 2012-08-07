package flipkart.platform.hydra.node.workstation;

import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.queue.MessageCtx;

/**
 * A {@link AbstractNode} that accepts and executes {@link OneToOneJob}.
 *
 * @author shashwat
 */

public class OneToOneWorkStation<I, O> extends AbstractNode<I, O, OneToOneJob<I, O>>
{
    public OneToOneWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        RetryPolicy<I> retryPolicy, JobFactory<? extends OneToOneJob<I, O>> jobFactory)
    {
        super(name, executorService, queue, retryPolicy, jobFactory);
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
            final MessageCtx<I> messageCtx = queue.read();
            final I i = messageCtx.get();
            try
            {
                try
                {
                    final O o = job.execute(i);
                    ackMessage(messageCtx, o);
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

        private void ackMessage(MessageCtx<I> messageCtx, O output)
        {
            if (output != null)
            {
                sendForward(output);
            }
            messageCtx.ack();
        }

    }
}
