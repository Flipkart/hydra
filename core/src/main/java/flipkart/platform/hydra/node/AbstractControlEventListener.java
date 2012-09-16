package flipkart.platform.hydra.node;

/**
 * User: shashwat
 * Date: 08/08/12
 */
public abstract class AbstractControlEventListener<T> implements NodeEventListener<T>
{
    @Override
    public void onNewMessage(Node<?, ? extends T> node, T i)
    {
    }
}
