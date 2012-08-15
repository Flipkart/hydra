package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public class LinkBuilder<O>
{
    private Link<O> link;

    public LinkBuilder(Selector<O> selector)
    {
        this.link = new DefaultLink<O>(selector);
    }

    public LinkBuilder()
    {
        this.link = new DefaultLink<O>();
    }

    public LinkBuilder(Link<O> link)
    {
        this.link = link;
    }

    public static <O> LinkBuilder<O> using(Selector<O> selector)
    {
        return new LinkBuilder<O>(selector);
    }

    public static <O> LinkBuilder<O> using(Link<O> link)
    {
        return new LinkBuilder<O>(link);
    }

    public static <O> LinkBuilder<O> link(Node<?, O>... fromNodes)
    {
        final LinkBuilder<O> linkBuilder = new LinkBuilder<O>();
        linkBuilder.linkFrom(fromNodes);
        return linkBuilder;
    }

    public <O1> LinkBuilder<O1> to(Node<O, O1> node)
    {
        link.addConsumer(node);
        return link(node);
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
