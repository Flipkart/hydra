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

    // TODO: Command pattern: API design - terminal APIs -> how to model
    boolean ack();

    int retry(int maxAttempts) throws NoMoreRetriesException;

    void discard();
    
    I get();

    int getAttempt();

    //int getAttempt();

    //State getState();
}
