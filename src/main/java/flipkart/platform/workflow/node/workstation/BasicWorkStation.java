package flipkart.platform.workflow.node.workstation;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.job.OneToOneJob;
import flipkart.platform.workflow.node.Node;

/**
 * A {@link WorkStation} that accepts and executes {@link OneToOneJob}.
 * 
 * @author shashwat
 * 
 */

public class BasicWorkStation<I, O> extends WorkStation<I, O, BasicJob<I, O>>
{
    private final Map<String, Node<O, ?>> nodes = new ConcurrentHashMap<String, Node<O, ?>>();

    public BasicWorkStation(final String name, int numThreads,
            final byte maxAttempts,
            final JobFactory<? extends BasicJob<I, O>> jobFactory)
    {
        super(name, numThreads, maxAttempts, jobFactory);
    }

    @Override
    public void append(Node<O, ?> node)
    {
        nodes.put(node.getName(), node);
    }

    @Override
    protected void acceptEntity(Entity<I> e)
    {
        queue.add(e);
        threadPool.execute(new BasicWorker());
    }

    public static <I, O> BasicWorkStation<I, O> create(String name,
            int numThreads, int maxAttempts,
            JobFactory<? extends BasicJob<I, O>> jobFactory)
    {
        return new BasicWorkStation<I, O>(name, numThreads, (byte) maxAttempts,
                jobFactory);
    }

    private class BasicWorker extends Worker
    {
        @Override
        protected void execute(BasicJob<I, O> job)
        {
            final Entity<I> e = pickEntity();
            try
            {
                job.execute(e.i, BasicWorkStation.this,
                        Collections.unmodifiableMap(nodes));
            }
            catch (Exception ex)
            {
                job.failed(e.i, ex);
            }
        }

    }

}