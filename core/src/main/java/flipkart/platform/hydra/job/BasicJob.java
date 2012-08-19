package flipkart.platform.hydra.job;

import flipkart.platform.hydra.node.JobContext;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(I i, JobContext<I, O, BasicJob<I, O>> jobContext);
}
