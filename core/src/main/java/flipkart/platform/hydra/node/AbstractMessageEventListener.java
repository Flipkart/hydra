package flipkart.platform.hydra.node;

/**
 * User: shashwat
 * Date: 08/08/12
 */
public abstract class AbstractMessageEventListener<T>implements NodeEventListener<T>
{
    @Override
    public void onShutdown(Node<?, T> tNode, boolean awaitTermination) throws InterruptedException
    {
        // nothing
    }
}
