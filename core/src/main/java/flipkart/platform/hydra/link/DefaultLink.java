package flipkart.platform.hydra.link;

import java.util.Collection;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * A class that implements default {@link Link} that can have more than one attached nodes and can onNewMessage messages to
 * one or more nodes depending if a {@link Selector}. Default behavior is to send messages to all the nodes.
 *
 * @author shashwat
 */
public class DefaultLink<T> extends AbstractLink<T, T> implements Link<T>
{
    public static <T> DefaultLink<T> from(Selector<T> selector)
    {
        return new DefaultLink<T>(selector);
    }

    public DefaultLink()
    {
        super();
    }

    public DefaultLink(Selector<T> selector)
    {
        super(selector);
    }

    @Override
    protected boolean forward(Node<?, ? extends T> node, T t)
    {
        return send(t);
    }
}
