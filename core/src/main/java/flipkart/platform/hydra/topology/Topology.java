package flipkart.platform.hydra.topology;

import flipkart.platform.hydra.node.Node;

/**
 * User: shashwat
 * Date: 17/08/12
 */
interface Topology
{
    <I> void connect(Node<?, I> from, Node<I, ?> to);

    <I> void disconnect(Node<?, I> from, Node<I, ?> to);

    void shutdown(boolean awaitTermination);

    boolean isShutdown();
}
