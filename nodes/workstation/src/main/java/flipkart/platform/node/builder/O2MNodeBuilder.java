package flipkart.platform.node.builder;

import flipkart.platform.node.jobs.OneToManyJob;
import flipkart.platform.node.workstation.OneToManyWorkStation;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.node.Node;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class O2MNodeBuilder<I, O> extends AbstractNodeBuilder<I, O>
{
    private final JobFactory<? extends OneToManyJob<I, O>> jobFactory;

    public O2MNodeBuilder(JobFactory<? extends OneToManyJob<I, O>> jobFactory)
    {
        this.jobFactory = jobFactory;
    }

    @Override
    public Node<I, O> build()
    {
        return new OneToManyWorkStation<I, O>(name, numThreads, queue, retryPolicy, jobFactory, link);
    }
}
