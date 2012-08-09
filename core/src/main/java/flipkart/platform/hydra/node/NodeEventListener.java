package flipkart.platform.hydra.node;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public interface NodeEventListener<T>
{
    public void onNewMessage(T i);

    public void onShutdown(Node<?, T> node, boolean awaitTermination) throws InterruptedException;
}
