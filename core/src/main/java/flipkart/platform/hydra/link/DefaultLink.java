package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.topology.LinkTopology;

/**
 * A class that implements default {@link Link} that can have more than one attached nodes and can onNewMessage messages to
 * one or more nodes depending if a {@link Selector}. Default behavior is to send messages to all the nodes.
 *
 * @author shashwat
 */
public class DefaultLink<T> extends AbstractLink<T, T> implements Link<T>
{
    public static <T> DefaultLink<T> using(LinkTopology topology, Selector<T> selector)
    {
        return new DefaultLink<T>(topology, selector);
    }

    public DefaultLink(LinkTopology topology)
    {
        super(topology);
    }

    public DefaultLink(LinkTopology topology, Selector<T> selector)
    {
        super(topology, selector);
    }

    @Override
    protected boolean forward(Node<?, ? extends T> node, T t)
    {
        return send(t);
    }
}
