package flipkart.platform.workflow.job;

/**
 * Exception to indicate execution failure in a job. The failures indicated by
 * this exception will be retried, if configured so.
 * 
 * @author shashwat
 * 
 */
public class ExecutionFailureException extends Exception
{
    public ExecutionFailureException(String message)
    {
        super(message);
    }

    public ExecutionFailureException(Throwable cause)
    {
        super(cause);
    }

    public ExecutionFailureException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
