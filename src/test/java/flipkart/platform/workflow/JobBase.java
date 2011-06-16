package flipkart.platform.workflow;

import flipkart.platform.workflow.job.Initializable;

public class JobBase<I> implements Initializable
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
        System.out.println("Job " + i + " failed: " + cause.getMessage());
    }
}