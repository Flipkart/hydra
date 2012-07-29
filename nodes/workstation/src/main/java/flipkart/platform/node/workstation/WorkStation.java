package flipkart.platform.node.workstation;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import flipkart.platform.workflow.job.Initializable;
import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;

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
 * @param <J>
 *     {@link flipkart.platform.workflow.job.Job} type
 * @author shashwat
 */
public abstract class WorkStation<I, O, J extends Job<I>> extends AbstractWorkStation<I, O>
{
    private final ThreadLocalJobRepository<I, J> threadLocalJobRepository;

    public WorkStation(String name, int numThreads, int maxAttempts, final JobFactory<? extends J> jobFactory)
    {
        this(name, numThreads, new DefaultRetryPolicy<I, O>(maxAttempts), jobFactory);
    }

    public WorkStation(String name, int numThreads, RetryPolicy<I, O> retryPolicy,
        final JobFactory<? extends J> jobFactory)
    {
        this(name, numThreads, retryPolicy, new ThreadLocalJobRepository<I, J>(jobFactory));
    }

    protected WorkStation(String name, int numThreads, RetryPolicy<I, O> retryPolicy,
        ThreadLocalJobRepository<I, J> threadLocalJobRepository)
    {
        super(name, numThreads, retryPolicy, new JobThreadFactory<I, J>(name, threadLocalJobRepository));

        this.threadLocalJobRepository = threadLocalJobRepository;
    }

    public AnyNode<I, O> anyNode(Class<I> iClass, Class<O> oClass)
    {
        return new AnyNode<I, O>(this, iClass, oClass);
    }

    public AnyNode<?, ?> anyNode()
    {
        return new AnyNode<I, O>(this);
    }

    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        super.shutdownResources(awaitTermination);
        threadLocalJobRepository.close();
    }

    public abstract class Worker extends WorkerBase
    {
        public void execute()
        {
            final J job = threadLocalJobRepository.get();
            if (job != null)
            {
                execute(job);
            }
        }

        protected abstract void execute(J job);

        public String getName()
        {
            return WorkStation.this.getName();
        }
    }

    protected static class ThreadLocalJobRepository<I, J extends Job<I>> extends ThreadLocal<J>
    {
        private final JobFactory<? extends J> jobFactory;

        public ThreadLocalJobRepository(JobFactory<? extends J> jobFactory)
        {
            this.jobFactory = jobFactory;
            Initializable.LifeCycle.initialize(jobFactory);
        }

        @Override
        protected J initialValue()
        {
            return jobFactory.newJob();
        }

        public void close()
        {
            Initializable.LifeCycle.destroy(jobFactory);
        }
    }

    protected static class JobThreadFactory<I, J extends Job<I>> implements ThreadFactory
    {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String name;
        private final ThreadLocal<J> threadLocal;

        public JobThreadFactory(String name, ThreadLocal<J> threadLocal)
        {
            this.name = name;
            this.threadLocal = threadLocal;
        }

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
                            System.out.println("Unable to initialize job, quitting");
                            threadLocal.set(null);
                            throw e;
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
