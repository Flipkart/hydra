package flipkart.platform.workflow.queue;

/**
 * Exception, raised when all tries are exhausted.
 * 
 * @author shashwat
 * 
 */
public class NoMoreRetriesException extends Exception
{
    public NoMoreRetriesException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public NoMoreRetriesException(String message)
    {
        super(message);
    }

    public NoMoreRetriesException(Throwable cause)
    {
        super(cause);
    }
}
