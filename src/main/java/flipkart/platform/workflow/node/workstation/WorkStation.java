package flipkart.platform.workflow.node.workstation;

import flipkart.platform.workflow.job.Initializable;
import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;

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
 * @author shashwat
 *
 * @param <I>
 *            Input job description type
 * @param <O>
 *            Output job description type
 * @param <J>
 *            {@link Job} type
 */
abstract class WorkStation<I, O, J extends Job<I>> implements Node<I, O>
{
    protected final ConcurrentLinkedQueue<Entity<I>> queue;
    protected final ThreadPoolExecutor threadPool;

    private final ThreadLocal<J> threadLocal;

    private final String name;
    private final JobFactory<? extends J> jobFactory;

    private final int maxAttempts;

    public WorkStation(final String name, int numThreads,
            final int maxAttempts, final JobFactory<? extends J> jobFactory)
    {
        this.name = name;
        this.jobFactory = jobFactory;
        this.threadLocal = new ThreadLocal<J>() {
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

    public abstract void append(Node<O, ?> node);

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void accept(I i)
    {
        acceptEntity(Entity.wrap(i));
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

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        threadPool.shutdown();
        while (awaitTermination
                && !threadPool.awaitTermination(10, TimeUnit.MILLISECONDS))
            ;
        Initializable.LifeCycle.destroy(jobFactory);
    }

    protected abstract void acceptEntity(Entity<I> e);

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

    protected abstract class Worker implements Runnable
    {
        public void run()
        {
            final J job = threadLocal.get();
            if (job != null)
            {
                execute(job);
            }
        }

        protected abstract void execute(J job);
    }

    protected class JobThreadFactory implements ThreadFactory
    {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public Thread newThread(Runnable r)
        {
            return new Thread(r, name + "-" + threadNumber.getAndIncrement()) {

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
