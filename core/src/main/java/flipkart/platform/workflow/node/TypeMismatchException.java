package flipkart.platform.workflow.node;

/**
 * Exception to indicate type mismatch. Used in {@link AnyNode} to indicate
 * incompatible node or input type being used.
 * 
 * @author shashwat
 * 
 */
public class TypeMismatchException extends RuntimeException
{
    public TypeMismatchException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public TypeMismatchException(String message)
    {
        super(message);
    }

    public TypeMismatchException(Throwable cause)
    {
        super(cause);
    }

}
