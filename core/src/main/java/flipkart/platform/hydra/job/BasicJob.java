package flipkart.platform.hydra.job;

import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(MessageCtx<I> i, JobExecutionContext<I, O, BasicJob<I, O>> jobExecutionContext);
}
