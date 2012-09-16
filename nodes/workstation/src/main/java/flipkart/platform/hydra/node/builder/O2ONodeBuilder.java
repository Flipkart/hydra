package flipkart.platform.hydra.node.builder;

import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.workstation.OneToOneWorkStation;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class O2ONodeBuilder<I, O> extends WorkStationBuilder<I, O>
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
        return new OneToOneWorkStation<I, O>(name, executorService, queue, retryPolicy, jobFactory);
    }
}
