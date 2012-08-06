package flipkart.platform.hydra.job;

import flipkart.platform.hydra.link.Link;
import flipkart.platform.hydra.node.Node;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(I i, Node<I, O> fromNode, Link<O> link);
}
