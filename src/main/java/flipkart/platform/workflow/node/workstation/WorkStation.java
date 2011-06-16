package flipkart.platform.workflow.node.workstation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import flipkart.platform.workflow.job.Initializable;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;

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
 * @param <Job>
 *            {@link Initializable} type
 */
abstract class WorkStation<I, O, Job extends Initializable> implements
        Node<I, O>
{
    protected final ConcurrentLinkedQueue<Entity<I>> queue;
    protected final ThreadPoolExecutor threadPool;

    private final ThreadLocal<Job> threadLocal;

    private final String name;
    private final Link<O> link;

    private final byte maxAttempts;

    public WorkStation(final String name, int numThreads,
            final byte maxAttempts, final JobFactory<Job> jobFactory,
            Link<O> link)
    {
        this.name = name;
        this.threadLocal = new ThreadLocal<Job>() {
            @Override
            protected Job initialValue()
            {
                return jobFactory.newJob();
            }
        };

        this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 0,
                TimeUnit.MICROSECONDS, new LinkedBlockingQueue<Runnable>(),
                new JobThreadFactory());
        this.maxAttempts = maxAttempts;
        this.queue = new ConcurrentLinkedQueue<Entity<I>>();
        this.link = link;
    }

    public void append(Node<O, ?> node)
    {
        link.append(node);
    }

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
    public void shutdown(boolean awaitTerminataion) throws InterruptedException
    {
        threadPool.shutdown();
        while (awaitTerminataion
                && !threadPool.awaitTermination(10, TimeUnit.MILLISECONDS))
            ;
        link.sendShutdown(awaitTerminataion);
    }

    protected abstract void acceptEntity(Entity<I> e);

    protected Entity<I> pickEntity()
    {
        return queue.poll();
    }

    protected void putEntity(Entity<O> e)
    {
        link.forward(e.i);
    }

    protected void putBack(Entity<I> e) throws NoMoreRetriesException
    {
        if (e.attempt < maxAttempts)
        {
            acceptEntity(e);
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
            final Job job = threadLocal.get();
            if (job != null)
            {
                execute(job);
            }
        }

        protected abstract void execute(Job job);
    }

    private class JobThreadFactory implements ThreadFactory
    {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public Thread newThread(Runnable r)
        {
            return new Thread(r, name + "-" + threadNumber.getAndIncrement()) {

                @Override
                public void run()
                {
                    Initializable job = null;

                    try
                    {
                        // initialization
                        try
                        {
                            job = threadLocal.get();
                            job.init();
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
                            job.destroy();
                        }
                    }
                }

            };
        }
    }

}
