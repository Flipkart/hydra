package flipkart.platform.node.workstation;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import flipkart.platform.node.jobs.OneToManyJob;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AbstractNode;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.queue.MessageCtx;

/**
 * A {@link AbstractNode} that executes {@link flipkart.platform.workflow.job
 * .OneToManyJob}
 *
 * @author shashwat
 */
public class OneToManyWorkStation<I, O> extends AbstractNode<I, O, OneToManyJob<I, O>>
{
    public OneToManyWorkStation(String name, ExecutorService executorService, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends OneToManyJob<I, O>> jobFactory, Link<O> oLink)
    {
        super(name, executorService, queue, retryPolicy, jobFactory, oLink);
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
