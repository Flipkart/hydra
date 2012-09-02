package flipkart.platform.hydra.node;

import flipkart.platform.hydra.common.MessageCtx;

/**
 * User: shashwat
 * Date: 29/07/12
 */

/**
 * RetryPolicy governs the retry if a job fails.
 *
 * @param <I>
 *     Input type
 * @see flipkart.platform.hydra.utils.DefaultRetryPolicy
 */
public interface RetryPolicy<I>
{
    /**
     * Given a messageCtx and the node that generated it, check if the message can be retried or not. If it cannot,
     * then throw NoMoreRetriesException.
     *
     * @param messageCtx MessageCtx of the message that needs to be retried
     * @return <code>true</code> if retried, <code>false</code> if no more retries possible
     */
    boolean retry(MessageCtx<I> messageCtx);
}
