package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public interface LinkEventListener
{
    public void onProducerNodeAdded(GenericLink link, Node node);

    public void onProducerNodeRemoved(GenericLink link, Node node);

    public void onConsumerNodeAdded(GenericLink link, Node node);

    public void onConsumerNodeRemoved(GenericLink link, Node node);
}
