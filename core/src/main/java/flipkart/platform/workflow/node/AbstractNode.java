package flipkart.platform.workflow.node;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import com.yammer.metrics.annotation.Timed;
import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.JobObjectFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.utils.RefCounter;
import flipkart.platform.workflow.utils.ThreadLocalRepository;

/**
 * An abstract {@link flipkart.platform.workflow.node.Node} implementation which executes job eventually using a
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
    protected final Link<O> link;
    private final ThreadLocalRepository<J> threadLocalJobRepository;
    private final String name;

    private volatile RunState state = RunState.ACTIVE;
    private final RefCounter activeWorkers = new RefCounter(0);

    protected AbstractNode(String name, HQueue<I> queue, ExecutorService executorService,
        JobFactory<? extends J> jobFactory, Link<O> link)
    {
        this.name = name;

        this.queue = queue;
        this.executorService = executorService;
        this.link = link;
        //this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 0,
        //    TimeUnit.MICROSECONDS, new LinkedBlockingQueue<Runnable>(), jobThreadFactory);
        this.threadLocalJobRepository = ThreadLocalRepository.from(JobObjectFactory.from(jobFactory));
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void append(Node<O, ?> node)
    {
        link.append(node);
    }

    @Override
    public void accept(I i)
    {
        if (state == RunState.ACTIVE)
        {
            queue.enqueue(i);
            scheduleWorker();
        }
        else
        {
            throw new RuntimeException("accept() called after shutdown()");
        }
    }

    @Override
    public final void shutdown(boolean awaitTermination) throws InterruptedException
    {
        state = RunState.SHUTTING_DOWN;

        // loop and check if there are no jobs in the queue and no workers executing any job
        while (awaitTermination && !isDone())
        {
            Thread.sleep(10);
        }

        shutdownResources(awaitTermination);
        state = RunState.SHUTDOWN;
    }

    public boolean isDone()
    {
        return (queue.isEmpty() && activeWorkers.isZero());
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
        link.sendShutdown(awaitTermination);
    }

    protected void sendForward(O o)
    {
        link.forward(o);
    }
    
    protected abstract void scheduleWorker();

    public abstract class WorkerBase implements Runnable
    {
        @Timed
        public void run()
        {
            activeWorkers.offer();
            try
            {
                final J j = threadLocalJobRepository.get();
                execute(j);
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
    }

}
