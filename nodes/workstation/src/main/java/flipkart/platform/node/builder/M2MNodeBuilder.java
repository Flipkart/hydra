package flipkart.platform.node.builder;

import flipkart.platform.node.jobs.ManyToManyJob;
import flipkart.platform.node.workstation.ManyToManyWorkStation;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.node.Node;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class M2MNodeBuilder<I, O> extends AbstractNodeBuilder<I, O>
{
    private final JobFactory<? extends ManyToManyJob<I, O>> jobFactory;
    private int maxJobsToGroup = 10;
    private long maxDelayMs;

    public M2MNodeBuilder(JobFactory<? extends ManyToManyJob<I, O>> jobFactory)
    {
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
        return new ManyToManyWorkStation<I, O>(name, numThreads, queue, retryPolicy, jobFactory, link, maxJobsToGroup,
            maxDelayMs);
    }
}
