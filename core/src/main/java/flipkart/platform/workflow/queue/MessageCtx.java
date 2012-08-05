package flipkart.platform.workflow.queue;

/**
 * User: shashwat
 * Date: 23/07/12
 */
public interface MessageCtx<I>
{
    enum DiscardAction
    {
        ENQUEUE, REJECT
    }

    boolean ack();

    int retry();

    void discard(DiscardAction discardAction);
    
    I get();

    int getAttempt();
}
