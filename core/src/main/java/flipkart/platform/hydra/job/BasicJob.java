package flipkart.platform.hydra.job;

import flipkart.platform.hydra.common.JobExecutionContext;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(I i, JobExecutionContext<I, O, BasicJob<I, O>> jobExecutionContext);
}
