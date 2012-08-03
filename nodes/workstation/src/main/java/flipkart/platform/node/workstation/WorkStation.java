package flipkart.platform.node.workstation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AbstractNode;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.HQueue;

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
public abstract class WorkStation<I, O, J extends Job<I>> extends AbstractNode<I, O, J>
{
    protected final RetryPolicy<I> retryPolicy;

    protected WorkStation(String name, int numThreads, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends J> jobFactory, Link<O> link)
    {
        this(name, Executors.newFixedThreadPool(numThreads, new JobThreadFactory(name)), queue, retryPolicy,
            jobFactory, link);
    }

    protected WorkStation(String name, ExecutorService executorService, HQueue<I> queue, RetryPolicy<I> retryPolicy,
        JobFactory<? extends J> jobFactory, Link<O> link)
    {
        super(name, queue, executorService, jobFactory, link);
        this.retryPolicy = retryPolicy;
    }
}
