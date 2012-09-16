package flipkart.platform.hydra.traits;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public interface CanShutdown
{
    boolean isShutdown();

    boolean shutdown(boolean awaitTermination);
}
