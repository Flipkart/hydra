package flipkart.platform.node.workstation;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.yammer.metrics.annotation.Timed;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.ConcurrentQueue;
import flipkart.platform.workflow.queue.Queue;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;
import flipkart.platform.workflow.utils.RefCounter;

/**
 * An abstract {@link flipkart.platform.workflow.node.Node} implementation which executes job eventually using a
 * {@link java.util.concurrent.ThreadPoolExecutor}. The jobs are created once for each thread. On
 * creation, jobs are initialized before being executed, and destroyed when the
 * thread is terminating because either the node is shutdown or is idle for long
 * as per the thread pool policies.
 *
 * @param <I>
 *     Input job description type
 * @param <O>
 *     Output job description type
 * @author shashwat
 */
public abstract class AbstractWorkStation<I, O> implements Node<I, O>
{
    public static enum RunState
    {
        ACTIVE,
        SHUTTING_DOWN,
        SHUTDOWN
    }

    protected final Queue<I> queue;
    protected final RetryPolicy<I, O> retryPolicy;

    private final ThreadPoolExecutor threadPool;
    private final String name;

    private volatile RunState state = RunState.ACTIVE;
    private final RefCounter activeWorkers = new RefCounter(0);

    protected AbstractWorkStation(String name, int numThreads, int maxAttempts, ThreadFactory jobThreadFactory)
    {
        this(name, numThreads, new DefaultRetryPolicy<I, O>(maxAttempts), jobThreadFactory);
    }

    protected AbstractWorkStation(String name, int numThreads, RetryPolicy<I, O> retryPolicy,
        ThreadFactory jobThreadFactory)
    {
        this.name = name;
        this.retryPolicy = retryPolicy;

        this.queue = new ConcurrentQueue<I>();
        this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 0,
            TimeUnit.MICROSECONDS, new LinkedBlockingQueue<Runnable>(), jobThreadFactory);
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void accept(I i)
    {
        if (state == RunState.ACTIVE)
        {
            acceptEntity(i);
        }
        else
        {
            throw new RuntimeException("accept() called after shutdown()");
        }
    }

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
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

    protected final void acceptEntity(I i)
    {
        queue.enqueue(i);
        scheduleWorker();
    }

    protected void executeWorker(WorkerBase worker)
    {
        threadPool.execute(worker);
    }

    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        threadPool.shutdown();
        while (awaitTermination
            && !threadPool.awaitTermination(10, TimeUnit.MILLISECONDS))
            ;
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
                execute();
            }
            finally
            {
                activeWorkers.take();
            }
        }

        protected abstract void execute();
    }

}
