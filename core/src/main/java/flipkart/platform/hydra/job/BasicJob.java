package flipkart.platform.hydra.job;

import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;

/**
 * Basic job that takes care of reporting job life cycle
 * @param <I> Input type
 * @param <O> Output type
 */
public interface BasicJob<I, O> extends Job<I>
{
    public void execute(MessageCtx<I> i, JobExecutionContext<I, O, BasicJob<I, O>> jobExecutionContext);
}
