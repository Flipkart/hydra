package flipkart.platform.hydra.node;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public interface NodeEventListener<T>
{
    public void onNewMessage(Node<?, ? extends T> node, T i);

    public void onShutdown(Node<?, ?> node, boolean awaitTermination) throws InterruptedException;
}
