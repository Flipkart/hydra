package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;

/**
 * Link is an interface that defines how two {@link Node}s are connected with
 * each other.
 *
 * @param <T> Object type that needs to be transferred between nodes
 * @author shashwat
 */
public interface Link<T> extends GenericLink<T, T>
{
}
