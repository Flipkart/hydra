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

    protected final JobExecutionContextFactory jobExecutionContextFactory;

    protected AbstractNode(String name, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends J> jobFactory)
    {
        super(name);
        this.queue = queue;
        this.retryPolicy = retryPolicy;

        this.threadLocalJobRepository = ThreadLocalRepository.from(JobObjectFactory.from(jobFactory));
        this.jobExecutionContextFactory = new JobExecutionContextFactory(this);
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

    public static class JobExecutionContextFactory<I, O, J extends Job<I>> implements
        JobExecutionContext.Factory<I, O, J>
    {
        private final AbstractNode<I, O, J> abstractNode;

        public JobExecutionContextFactory(AbstractNode<I, O, J> abstractNode)
        {
            this.abstractNode = abstractNode;
        }

        public JobExecutionContext<I, O, J> newJobExecutionContext()
        {
            final J j = abstractNode.threadLocalJobRepository.get();
            if (j != null)
            {
                return new NodeJobExecutionContext(j, abstractNode);
            }
            return null;
        }
    }

    protected static class NodeJobExecutionContext<I, O, J extends Job<I>> extends AbstractJobExecutionContext<I, O, J>
    {
        private final AbstractNode<I, O, J> abstractNode;

        public NodeJobExecutionContext(J j, AbstractNode<I, O, J> abstractNode)
        {
            super(j, abstractNode.getIdentity(), abstractNode.retryPolicy);
            this.abstractNode = abstractNode;

            begin();
        }

        private void begin()
        {
            abstractNode.activeWorkers.offer();
            metrics.reportJobStart();
        }

        @Override
        public void end()
        {
            metrics.reportJobEnd();
            abstractNode.activeWorkers.take();
        }

        @Override
        public void submitResponse(O o)
        {
            if (o != null)
            {
                abstractNode.sendForward(o);
            }
        }

        @Override
        protected void scheduleRetryJob()
        {
            abstractNode.scheduleJob();
        }
    }
}
