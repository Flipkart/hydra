package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public interface LinkEventListener
{
    public void onProducerNodeAdded(Link link, Node node);

    public void onProducerNodeRemoved(Link link, Node node);

    public void onConsumerNodeAdded(Link link, Node node);

    public void onConsumerNodeRemoved(Link link, Node node);
}
