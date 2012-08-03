package flipkart.platform.workflow;

import flipkart.platform.hydra.traits.Initializable;
import flipkart.platform.workflow.job.Job;

public class JobBase<I> implements Job<I>, Initializable
{
    @Override
    public void init()
    {
    }

    @Override
    public void destroy()
    {
    }

    public void failed(I i, Throwable cause)
    {
        System.out.println("Job " + i + " failed");
        cause.printStackTrace();
    }
}
