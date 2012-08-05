package flipkart.platform.node.workstation;

import java.util.concurrent.ExecutorService;
import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AbstractNode;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.queue.MessageCtx;

/**
 * A {@link AbstractNode} that accepts and executes {@link
 * flipkart.platform.node.jobs.OneToOneJob}.
 *
 * @author shashwat
 */

public class OneToOneWorkStation<I, O> extends AbstractNode<I, O, OneToOneJob<I, O>>
{
    public OneToOneWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        RetryPolicy<I> retryPolicy, JobFactory<? extends OneToOneJob<I, O>> jobFactory, Link<O> oLink)
    {
        super(name, executorService, queue, retryPolicy, jobFactory, oLink);
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
