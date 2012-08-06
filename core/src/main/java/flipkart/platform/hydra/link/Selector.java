package flipkart.platform.hydra.link;

import java.util.Collection;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * Selector interface to choose one or more nodes
 *
 * @author shashwat
 */
public interface Selector<T>
{
    /**
     * Select one or more nodes to forward the message to.
     *
     *
     * @param i
     *     Job description to be forwarded
     * @param nodes
     *     {@link flipkart.platform.hydra.node.Node}s registered with this link
     * @return List of nodes to forward to. An empty or null return value
     *         will cause the job to be discarded.
     */
    public Collection<Node<T, ?>> select(T i, UnModifiableMap<String, Node<T, ?>> nodes);
}
