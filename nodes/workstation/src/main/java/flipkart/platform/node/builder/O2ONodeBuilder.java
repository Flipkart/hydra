package flipkart.platform.node.builder;

import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.node.workstation.OneToOneWorkStation;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.node.AbstractNodeBuilder;
import flipkart.platform.workflow.node.Node;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class O2ONodeBuilder<I, O> extends AbstractNodeBuilder<I, O>
{
    private final JobFactory<? extends OneToOneJob<I, O>> jobFactory;

    public O2ONodeBuilder(String name, JobFactory<? extends OneToOneJob<I, O>> jobFactory)
    {
        super(name);
        this.jobFactory = jobFactory;
    }

    @Override
    public Node<I, O> build()
    {
        return new OneToOneWorkStation<I, O>(name, executorService, queue, retryPolicy, jobFactory, link);
    }
}
