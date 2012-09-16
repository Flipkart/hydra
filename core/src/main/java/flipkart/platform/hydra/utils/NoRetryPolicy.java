package flipkart.platform.hydra.utils;

import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.RetryPolicy;

/**
* User: shashwat
* Date: 29/07/12
*/
public class NoRetryPolicy<I> implements RetryPolicy<I>
{
    @Override
    public boolean retry(MessageCtx<I> messageCtx)
    {
        return false;
    }
}
