package flipkart.platform.workflow.node;

import flipkart.platform.workflow.job.BasicJob;
import flipkart.platform.workflow.job.Initializable;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import sun.net.www.content.audio.basic;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public class BasicNode<I, O> implements Node<I, O>
{
    private final String name;
    private final JobFactory<? extends BasicJob<I, O>> jobFactory;
    private final Link<O> link;

    public BasicNode(String name, JobFactory<? extends BasicJob<I, O>> jobFactory, Link<O> link)
    {
        this.name = name;
        this.jobFactory = jobFactory;
        this.link = link;

        Initializable.LifeCycle.initialize(jobFactory);
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void append(Node<O, ?> node)
    {
        link.append(node);
    }

    @Override
    public void accept(I i)
    {
        final BasicJob<I, O> basicJob = jobFactory.newJob();
        basicJob.execute(i, this, link);
    }

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        Initializable.LifeCycle.destroy(jobFactory);
    }
}
