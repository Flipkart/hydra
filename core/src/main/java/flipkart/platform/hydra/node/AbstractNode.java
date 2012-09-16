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
 * An abstract {@link flipkart.platform.hydra.node.Node} that extends {@link BaseNode} and provides framework
 * to process messages in either sync or async manner.
 * <p/>
 * Provides configuration in terms of:
 * <ul>
 * <li>{@link Job} that will process the message</li>
 * <li>{@link HQueue} that will store the messages</li>
 * <li>{@link RetryPolicy} that will be used to retry on failure</li>
 * </ul>
 * <p/>
 * In case of threaded, async execution, manages job and {@link JobFactory} life cycle if they are {@link
 * flipkart.platform.hydra.traits.Initializable}
 *
 * @param <I>
 *     Input job description type
 * @param <O>
 *     Output job description type
 * @param <J>
 *     {@link Job} type that will process the messages
 * @author shashwat
 */
public abstract class AbstractNode<I, O, J extends Job<I>> extends BaseNode<I, O>
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

    /**
     * @return <code>true</code> only if message queue is empty and there are no active workers
     */
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

    /**
     * Required to be implemented by the derived class to schedule the execution of the job. scheduleJob is called
     * whenever a new message is added to the message queue.
     */
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
