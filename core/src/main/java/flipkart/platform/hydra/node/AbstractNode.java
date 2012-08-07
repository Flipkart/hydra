package flipkart.platform.hydra.node;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.collect.Lists;
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
public abstract class AbstractNode<I, O, J extends Job<I>> implements Node<I, O>
{
    public static enum RunState
    {
        ACTIVE,
        SHUTTING_DOWN,
        SHUTDOWN
    }

    protected final HQueue<I> queue;

    private final ExecutorService executorService;
    private final RetryPolicy<I> retryPolicy;
    private final ThreadLocalRepository<J> threadLocalJobRepository;
    private final String name;

    private final AtomicReference<RunState> state = new AtomicReference<RunState>(RunState.ACTIVE);
    private final RefCounter activeWorkers = new RefCounter(0);

    private final List<NodeEventListener<O>> eventListeners = Lists.newLinkedList();

    protected AbstractNode(String name, ExecutorService executorService, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends J> jobFactory)
    {
        this.name = name;

        this.queue = queue;
        this.executorService = executorService;
        this.retryPolicy = retryPolicy;

        this.threadLocalJobRepository = ThreadLocalRepository.from(JobObjectFactory.from(jobFactory));
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void addListener(NodeEventListener<O> nodeListener)
    {
        eventListeners.add(nodeListener);
    }

    @Override
    public void accept(I i)
    {
        validateState();

        queue.enqueue(i);
        scheduleWorker();
    }

    public boolean isDone()
    {
        return (queue.isEmpty() && activeWorkers.isZero());
    }

    @Override
    public final void shutdown(boolean awaitTermination) throws InterruptedException
    {
        if (state.compareAndSet(RunState.ACTIVE, RunState.SHUTTING_DOWN))
        {
            // loop and check if there are no jobs in the queue and no workers executing any job
            while (awaitTermination && !isDone())
            {
                Thread.sleep(10);
            }

            shutdownResources(awaitTermination);
            state.set(RunState.SHUTDOWN);
            for (NodeEventListener<O> eventListener : eventListeners)
            {
                eventListener.onShutdown(this, awaitTermination);
            }
        }
        else
        {
            throw new RuntimeException("Shutdown already in progress");
        }
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

    protected final void validateState()
    {
        if (state.get() != RunState.ACTIVE)
        {
            throw new RuntimeException("accept() called after shutdown()");
        }
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
                eventListener.forward(o);
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
