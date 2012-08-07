package flipkart.platform.hydra.queue;

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

    /**
     * Timestamp when this message was enqueue into the queue. This is true for each retry as well.
     * @return Long timestamp
     */
    long getCreatedTimestamp();

}
