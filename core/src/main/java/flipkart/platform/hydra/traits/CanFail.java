package flipkart.platform.hydra.traits;

/**
 * Interface that identifies tasks that can eventually fail.
 *
 * @author shashwat
 */
public interface CanFail<I>
{
    /**
     * Called when job processing failed because of {@link flipkart.platform.hydra.job.ExecutionFailureException} and no more retries are available
     * or any other permanent failures indicated via RuntimeException.
     *
     * @param i
     *     Job description that failed
     * @param cause
     *     Cause of failure.
     */
    public void failed(I i, Throwable cause);
}
