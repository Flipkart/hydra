package flipkart.platform.hydra.common;

import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.Job;

/**
 * User: shashwat
 * Date: 27/08/12
 */
public interface JobExecutionContext<I, O, J extends Job<I>>
{
    /**
     * begin() is supposed to lazily create a job so as to facilitate thread local jobs where-ever required.
     * @return new job
     */

    J begin();

    void end(J j);

    void submitResponse(O o);

    void succeeded(J j, MessageCtx<I> messageCtx);

    void failed(J j, MessageCtx<I> messageCtx, Throwable t);
}
