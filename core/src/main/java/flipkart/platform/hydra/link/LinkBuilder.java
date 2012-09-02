package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.topology.LinkTopology;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public class LinkBuilder<O>
{
    private Link<O> link;

    public LinkBuilder(LinkTopology topology, Selector<O> selector)
    {
        this.link = new DefaultLink<O>(topology, selector);
    }

    public LinkBuilder(LinkTopology topology)
    {
        this.link = new DefaultLink<O>(topology);
    }

    public LinkBuilder(Link<O> link)
    {
        this.link = link;

    }

    public static <O> LinkBuilder<O> using(LinkTopology topology, Selector<O> selector)
    {
        return new LinkBuilder<O>(topology, selector);
    }

    public static <O> LinkBuilder<O> using(Link<O> link)
    {
        return new LinkBuilder<O>(link);
    }

    public static <O> LinkBuilder<O> link(LinkTopology topology, Node<?, O>... fromNodes)
    {
        final LinkBuilder<O> linkBuilder = new LinkBuilder<O>(topology);
        linkBuilder.linkFrom(fromNodes);
        return linkBuilder;
    }

    public <O1> LinkBuilder<O1> to(Node<O, O1> node)
    {
        link.addConsumer(node);
        return link(link.getTopology(), node);
    }

    public void toOnly(Node<O, ?> node)
    {
        link.addConsumer(node);
    }

    public void to(Node<O, ?>... nodes)
    {
        for (Node<O, ?> node : nodes)
        {
            link.addConsumer(node);
        }
    }

    public LinkBuilder<O> linkFrom(Node<?, O>... producerNodes)
    {
        for (Node<?, O> producerNode : producerNodes)
        {
            link.addProducer(producerNode);
        }
        return this;
    }
}
