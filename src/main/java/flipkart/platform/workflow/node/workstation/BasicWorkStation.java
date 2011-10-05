package flipkart.platform.workflow.node.workstation;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.OneToOneJob;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.Node;

/**
 * A {@link WorkStation} that accepts and executes {@link OneToOneJob}.
 *
 * @author shashwat
 */

public class BasicWorkStation<I, O> extends LinkBasedWorkStation<I, O, BasicJob<I, O>>
{
    public BasicWorkStation(String name, int numThreads, byte maxAttempts,
                            JobFactory<? extends BasicJob<I, O>> jobFactory, Link<O> oLink)
    {
        super(name, numThreads, maxAttempts, jobFactory, oLink);
    }

    @Override
    protected void acceptEntity(Entity<I> e)
    {
        queue.add(e);
        threadPool.execute(new BasicWorker());
    }

    public static <I, O> BasicWorkStation<I, O> create(String name,
            int numThreads, int maxAttempts,
            JobFactory<? extends BasicJob<I, O>> jobFactory, Link<O> link)
    {
        return new BasicWorkStation<I, O>(name, numThreads, (byte) maxAttempts,
                jobFactory, link);
    }

    private class BasicWorker extends Worker
    {
        @Override
        protected void execute(BasicJob<I, O> job)
        {
            final Entity<I> e = pickEntity();
            try
            {
                job.execute(e.i, BasicWorkStation.this, link);
            }
            catch (Exception ex)
            {
                job.failed(e.i, ex);
            }
        }

    }

}
