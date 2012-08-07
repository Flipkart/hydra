package flipkart.platform.hydra.node;

import flipkart.platform.hydra.job.Job;
import flipkart.platform.hydra.queue.MessageCtx;

/**
 * User: shashwat
 * Date: 07/08/12
 */

// TODO: More relevant name
public interface JobContext<I, O, J extends Job<I>>
{
    void sendForward(O o);

    void retryMessage(J j, MessageCtx<I> messageCtx, Throwable t);

    void discardMessage(J j, MessageCtx<I> messageCtx, Throwable t);
}
