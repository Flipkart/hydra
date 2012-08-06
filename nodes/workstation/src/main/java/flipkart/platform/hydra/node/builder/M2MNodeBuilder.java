package flipkart.platform.hydra.node.builder;

import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.workstation.ManyToManyWorkStation;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class M2MNodeBuilder<I, O> extends AbstractNodeBuilder<I, O>
{
    private final JobFactory<? extends ManyToManyJob<I, O>> jobFactory;
    private int maxJobsToGroup = 10;
    private long maxDelayMs;

    public M2MNodeBuilder(String name, JobFactory<? extends ManyToManyJob<I, O>> jobFactory)
    {
        super(name);
        this.jobFactory = jobFactory;
    }

    public M2MNodeBuilder<I, O> withBatch(int maxJobsToGroup, long maxDelays)
    {
        this.maxJobsToGroup = maxJobsToGroup;
        this.maxDelayMs = maxDelayMs;
        return this;
    }

    @Override
    public Node<I, O> build()
    {
        return new ManyToManyWorkStation<I, O>(name, executorService, queue, retryPolicy, jobFactory, link,
            maxJobsToGroup, maxDelayMs);
    }
}
