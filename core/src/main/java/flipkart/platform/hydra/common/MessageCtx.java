package flipkart.platform.hydra.common;

/**
 * A typical message life cycle looks like:
 * <p/>
 * ENQUEUE ----> READ/PROCESS ----> ACKNOWLEDGE
 * ...................|
 * ...................----> RETRY ---> REJECT
 * MessageCtx is an abstraction implemented by the message container (or generator) to provide methods to report and
 * handle message life cycle and at the same time hiding the nature of the message container.
 *
 * @param <I>
 */
public interface MessageCtx<I>
{
    /**
     * The action that needs to be performed.
     * <p/>
     * <ul>
     * <li>{@link DiscardAction#ENQUEUE}
     * <li>{@link DiscardAction#REJECT}
     * </ul>
     *
     * @see MessageCtx#discard(flipkart.platform.hydra.common.MessageCtx.DiscardAction)
     * @see flipkart.platform.hydra.common.MessageCtx#getAttempt()
     */
    enum DiscardAction
    {
        /**
         * Enqueue the message back to the queue without increasing the number of the attempts
         */
        ENQUEUE,
        /**
         * Discard the message. Message can no longer be processed because of too many failures. Container can either
         * choose to drop the message or can try to sideline the message for auditing if it can.
         */
        REJECT
    }

    /**
     * Acknowledge the successful completion of this message. On receiving the acknowledgment, message can be safely
     * removed from the container.
     *
     * @return <code>true</code> if the message was successfully acknowledged by the container, <code>false</code>
     *         otherwise.
     */
    boolean ack();

    /**
     * Retry (enqueue it back into the queue) this message as its processing failed.
     *
     * @return the number of times the message was added to the queue (including this retry). The least value returned
     *         by this method is 2 indicating that the message was added twice into the queue.
     */
    int retry();

    /**
     * Discard the message. Used to notify that the message cannot be processed either because of too many attempts or
     * it was detected that message needs to be processed later and hence add the message back into the queue.
     * <p/>
     * If the message is
     *
     * @param discardAction
     *     required {@link DiscardAction}
     */
    void discard(DiscardAction discardAction);

    /**
     * @return the message contained in this context instance
     */
    I get();

    /**
     * An attempt is said to have been made if the message is going to be processed. Loosely,
     * when the message was first added to the queue, that was the first attempt. The subsequent retries increment the
     * attempts. {@link #discard(flipkart.platform.hydra.common.MessageCtx.DiscardAction)} with
     * <code>DiscardAction.Enqueue</code> as argument does not increase the number of attempts as the message was never
     * processed.
     *
     * @return the number of times the message was added into the queue. Without any {@link #retry()}, returns 1
     *         indicating the first attempt.
     */
    int getAttempt();

    /**
     * Timestamp when this message was enqueue into the queue. A new timestamp is assigned for each retry attempt as
     * well.
     *
     * @return Long timestamp
     */
    long getCreatedTimestamp();

}
