package flipkart.platform.hydra.topology;

import flipkart.platform.hydra.node.Node;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public interface NodeShutdownPolicy
{
    public void shutdown(Node node, boolean awaitTermination);
}
