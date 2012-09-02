package flipkart.platform.hydra.topology;

import flipkart.platform.hydra.node.Node;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public class DefaultNodeShutdownPolicy implements NodeShutdownPolicy
{
    @Override
    public void shutdown(Node node, boolean awaitTermination)
    {
        try
        {
            node.shutdown(awaitTermination);
        }
        catch (InterruptedException e)
        {
            // TODO:
        }
    }
}
