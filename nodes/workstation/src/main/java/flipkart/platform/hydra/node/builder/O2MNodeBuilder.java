package flipkart.platform.hydra.node.builder;

import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.OneToManyJob;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.workstation.OneToManyWorkStation;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class O2MNodeBuilder<I, O> extends AbstractNodeBuilder<I, O>
{
    private final JobFactory<? extends OneToManyJob<I, O>> jobFactory;

    public O2MNodeBuilder(String name, JobFactory<? extends OneToManyJob<I, O>> jobFactory)
    {
        super(name);
        this.jobFactory = jobFactory;
    }

    @Override
    public Node<I, O> build()
    {
        return new OneToManyWorkStation<I, O>(name, executorService, queue, retryPolicy, jobFactory);
    }
}
