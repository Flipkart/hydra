package flipkart.platform.workflow.queue;

/**
 * User: shashwat
 * Date: 23/07/12
 */
public interface MessageCtx<I>
{
    enum State
    {
        NEW, ACK, RETRY, DISCARD
    }

    enum DiscardAction
    {
        ENQUEUE, SIDELINE, REJECT
    }

    boolean ack();

    int retry();

    void discard(DiscardAction discardAction);
    
    I get();

    int getAttempt();
}
