package flipkart.platform.hydra.common;

import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.Job;
import flipkart.platform.hydra.metrics.NodeMetrics;
import flipkart.platform.hydra.node.RetryPolicy;

/**
 * User: shashwat
 * Date: 28/08/12
 */
public abstract class AbstractJobExecutionContext<I, O, J extends Job<I>> implements JobExecutionContext<I, O, J>
{
    private final RetryPolicy<I> retryPolicy;
    protected final NodeMetrics metrics;

    protected AbstractJobExecutionContext(String identity, RetryPolicy<I> retryPolicy)
    {
        this.retryPolicy = retryPolicy;
        this.metrics = new NodeMetrics(identity);
    }

    @Override
    public void succeeded(J j, MessageCtx<I> messageCtx)
    {
        messageCtx.ack();
        metrics.reportMessageProcessingTime(messageCtx.getCreatedTimestamp());
        metrics.reportMessageProcessed(NodeMetrics.Result.SUCCEEDED);
    }

    @Override
    public void failed(J j, MessageCtx<I> messageCtx, Throwable t)
    {
        if (!retryPolicy.retry(messageCtx))
        {
            metrics.reportMessageRetryAttempts(messageCtx.getAttempt());

            messageCtx.discard(MessageCtx.DiscardAction.REJECT);
            j.failed(messageCtx.get(), t);
        }
        metrics.reportMessageProcessed(NodeMetrics.Result.FAILED);
    }
}
