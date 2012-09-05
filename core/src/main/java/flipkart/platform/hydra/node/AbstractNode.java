package flipkart.platform.hydra.node;

import flipkart.platform.hydra.common.AbstractJobExecutionContext;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.job.Job;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.job.JobObjectFactory;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.utils.RefCounter;
import flipkart.platform.hydra.utils.ThreadLocalRepository;

/**
 * An abstract {@link flipkart.platform.hydra.node.Node} implementation which executes job eventually using a
 * {@link java.util.concurrent.ThreadPoolExecutor}.
 *
 * @param <I>
 *     Input job description type
 * @param <O>
 *     Output job description type
 * @author shashwat
 */
public abstract class AbstractNode<I, O, J extends Job<I>> extends AbstractNodeBase<I, O>
{
    protected final HQueue<I> queue;

    private final RetryPolicy<I> retryPolicy;
    private final ThreadLocalRepository<J> threadLocalJobRepository;
    private final RefCounter activeWorkers = new RefCounter(0);

    protected AbstractNode(String name, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends J> jobFactory)
    {
        super(name);
        this.queue = queue;
        this.retryPolicy = retryPolicy;

        this.threadLocalJobRepository = ThreadLocalRepository.from(JobObjectFactory.from(jobFactory));
    }

    public boolean isDone()
    {
        return (super.isDone() && queue.isEmpty() && activeWorkers.isZero());
    }

    @Override
    protected void acceptMessage(I i)
    {
        queue.enqueue(i);
        scheduleJob();
    }

    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        threadLocalJobRepository.close();
    }

    protected abstract void scheduleJob();

    protected JobExecutionContext<I, O, J> newJobExecutionContext()
    {
        return new NodeJobExecutionContext(retryPolicy);
    }

    protected class NodeJobExecutionContext extends AbstractJobExecutionContext<I, O, J>
    {
        public NodeJobExecutionContext(RetryPolicy<I> retryPolicy)
        {
            super(getIdentity(), retryPolicy);
        }

        @Override
        public J begin()
        {
            final J j = threadLocalJobRepository.get();
            if (j != null)
            {
                activeWorkers.offer();
                metrics.reportJobStart();
            }
            return j;
        }

        @Override
        public void end(J j)
        {
            if (j != null)
            {
                metrics.reportJobEnd();
                activeWorkers.take();
            }
        }

        @Override
        public void submitResponse(O o)
        {
            if (o != null)
            {
                AbstractNode.this.sendForward(o);
            }
        }

        @Override
        protected void scheduleRetryJob()
        {
            scheduleJob();
        }
    }
}
