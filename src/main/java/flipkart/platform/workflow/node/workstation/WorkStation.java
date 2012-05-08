package flipkart.platform.workflow.node.workstation;

import flipkart.platform.workflow.job.Initializable;
import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.utils.RefCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An abstract {@link Node} implementation which executes job eventually using a
 * {@link ThreadPoolExecutor}. The jobs are created once for each thread. On
 * creation, jobs are initialized before being executed, and destroyed when the
 * thread is terminating because either the node is shutdown or is idle for long
 * as per the thread pool policies.
 *
 * @param <I>
 *     Input job description type
 * @param <O>
 *     Output job description type
 * @param <J>
 *     {@link Job} type
 * @author shashwat
 */
abstract class WorkStation<I, O, J extends Job<I>> implements Node<I, O>
{
    public static enum RunState
    {
        ACTIVE,
        SHUTTING_DOWN,
        SHUTDOWN
    }

    private final ConcurrentLinkedQueue<Entity<I>> queue;
    protected final ThreadPoolExecutor threadPool;

    private final ThreadLocal<J> threadLocal;

    private final String name;
    private final JobFactory<? extends J> jobFactory;

    private final int maxAttempts;

    private volatile RunState state = RunState.ACTIVE;
    private final RefCounter activeWorkers = new RefCounter(0);

    public WorkStation(final String name, int numThreads,
        final int maxAttempts, final JobFactory<? extends J> jobFactory)
    {
        this.name = name;
        this.jobFactory = jobFactory;
        this.threadLocal = new ThreadLocal<J>()
        {
            @Override
            protected J initialValue()
            {
                return jobFactory.newJob();
            }
        };

        this.maxAttempts = maxAttempts;
        this.queue = new ConcurrentLinkedQueue<Entity<I>>();

        Initializable.LifeCycle.initialize(jobFactory);

        this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 0,
            TimeUnit.MICROSECONDS, new LinkedBlockingQueue<Runnable>(),
            new JobThreadFactory());
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
            acceptEntity(Entity.wrap(i));
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

    public List<Entity<I>> getIncompleteJobs()
    {
        return new ArrayList<Entity<I>>(queue);
    }

    public AnyNode<I, O> anyNode(Class<I> iClass, Class<O> oClass)
    {
        return new AnyNode<I, O>(this, iClass, oClass);
    }

    public AnyNode<?, ?> anyNode()
    {
        return new AnyNode<I, O>(this);
    }

    protected final void acceptEntity(Entity<I> i)
    {
        queue.add(i);
        scheduleWorker();
    }

    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        threadPool.shutdown();
        while (awaitTermination
            && !threadPool.awaitTermination(10, TimeUnit.MILLISECONDS))
            ;
        Initializable.LifeCycle.destroy(jobFactory);
    }

    protected Entity<I> pickEntity()
    {
        return queue.poll();
    }

    protected void putBack(Entity<I> e) throws NoMoreRetriesException
    {
        if (e.attempt < maxAttempts)
        {
            acceptEntity(Entity.from(e));
        }
        else
        {
            throw new NoMoreRetriesException("Failed after attempts: "
                + e.attempt);
        }
    }

    protected abstract void scheduleWorker();

    protected abstract class Worker implements Runnable
    {
        public void run()
        {
            final J job = threadLocal.get();
            if (job != null)
            {
                activeWorkers.offer();
                try
                {
                    execute(job);
                }
                finally
                {
                    activeWorkers.take();
                }
            }
        }

        protected abstract void execute(J job);
    }

    protected class JobThreadFactory implements ThreadFactory
    {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public Thread newThread(Runnable r)
        {
            return new Thread(r, name + "-" + threadNumber.getAndIncrement())
            {

                @Override
                public void run()
                {
                    J job = null;
                    try
                    {
                        // initialization
                        job = threadLocal.get();
                        try
                        {
                            Initializable.LifeCycle.initialize(job);
                        }
                        catch (Exception e)
                        {
                            // TODO log
                            e.printStackTrace();
                            threadLocal.set(null);
                        }
                        // Processing
                        super.run();
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                    finally
                    {
                        if (job != null)
                        {
                            Initializable.LifeCycle.destroy(job);
                        }
                    }
                }

            };
        }
    }

}
