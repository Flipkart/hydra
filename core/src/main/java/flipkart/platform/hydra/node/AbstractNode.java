package flipkart.platform.hydra.node;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.yammer.metrics.annotation.Timed;
import flipkart.platform.hydra.job.Job;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.job.JobObjectFactory;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.queue.MessageCtx;
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

    private final ExecutorService executorService;
    private final RetryPolicy<I> retryPolicy;
    private final ThreadLocalRepository<J> threadLocalJobRepository;
    private final AtomicReference<RunState> state = new AtomicReference<RunState>(RunState.ACTIVE);
    private final RefCounter activeWorkers = new RefCounter(0);

    protected AbstractNode(String name, ExecutorService executorService, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends J> jobFactory)
    {
        super(name);
        this.queue = queue;
        this.executorService = executorService;
        this.retryPolicy = retryPolicy;

        this.threadLocalJobRepository = ThreadLocalRepository.from(JobObjectFactory.from(jobFactory));
    }

    public boolean isDone()
    {
        return (queue.isEmpty() && activeWorkers.isZero());
    }

    @Override
    protected void acceptMessage(I i)
    {
        queue.enqueue(i);
        scheduleWorker();
    }

    protected void executeWorker(WorkerBase worker)
    {
        executorService.execute(worker);
    }

    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        executorService.shutdown();
        while (awaitTermination
            && !executorService.awaitTermination(10, TimeUnit.MILLISECONDS))
            ;

        threadLocalJobRepository.close();

        // TODO: send shutdown
    }

    protected abstract void scheduleWorker();

    public abstract class WorkerBase implements Runnable, JobContext<I, O, J>
    {
        @Timed
        public void run()
        {
            activeWorkers.offer();
            final J j = threadLocalJobRepository.get();
            try
            {
                if (j != null)
                {
                    execute(j);
                }
            }
            finally
            {
                activeWorkers.take();
            }
        }

        public String getName()
        {
            return AbstractNode.this.getName();
        }

        protected abstract void execute(J j);

        public void sendForward(O o)
        {
            for (NodeEventListener<O> eventListener : eventListeners)
            {
                eventListener.onNewMessage(AbstractNode.this, o);
            }
        }

        public void retryMessage(J j, MessageCtx<I> messageCtx, Throwable t)
        {
            if (!retryPolicy.retry(AbstractNode.this, messageCtx))
            {
                discardMessage(j, messageCtx, t);
            }
            // TODO: log
        }

        public void discardMessage(J j, MessageCtx<I> messageCtx, Throwable t)
        {
            // TODO: log
            messageCtx.discard(MessageCtx.DiscardAction.REJECT);
            j.failed(messageCtx.get(), t);
        }
    }

}
