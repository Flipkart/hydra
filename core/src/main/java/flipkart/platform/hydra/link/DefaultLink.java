package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.topology.LinkTopology;

/**
 * Default implementation of {@link Link}. The output type from the producers is the same as expected in the input by
 * the consumers. In other words, default link does not transform the output of consumers in any way.
 *
 * @author shashwat
 * @see AbstractLink
 */
public class DefaultLink<T> extends AbstractLink<T, T>
{
    /**
     * Create a new DefaultLink in the given {@link LinkTopology} and the {@link Selector} link
     *
     * @param topology
     *     {@link LinkTopology} instance for which this link will be part of
     * @param selector
     *     {@link Selector} to use
     * @param <T>
     *     Type output by producer and expected as input from consumer
     * @return new {@link DefaultLink} instance
     */
    public static <T> DefaultLink<T> using(LinkTopology topology, Selector<T> selector)
    {
        return new DefaultLink<T>(topology, selector);
    }

    /**
     * Create new {@link DefaultLink} in given {@link LinkTopology}
     *
     * @param topology
     *     {@link LinkTopology} instance
     * @see AbstractLink#AbstractLink(flipkart.platform.hydra.topology.LinkTopology)
     */
    public DefaultLink(LinkTopology topology)
    {
        super(topology);
    }

    /**
     * * Create a new DefaultLink in the given {@link LinkTopology} and the {@link Selector} link
     *
     * @param topology
     *     {@link LinkTopology} instance for which this link will be part of
     * @param selector
     *     {@link Selector} to use
     * @see AbstractLink#AbstractLink(flipkart.platform.hydra.topology.LinkTopology, Selector)
     */
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
