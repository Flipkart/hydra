package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;

/**
 * Link is an interface that defines how two {@link Node}s are connected with
 * each other.
 *
 * @param <T> Object type that needs to be transferred between nodes
 * @author shashwat
 */
public interface Link<T>
{
    /**
     * Attach given node to it's end
     *
     * @param node
     *     {@link Node} to be attached
     */
    public <O> void addConsumer(Node<T, O> node);

    /**
    * Attach given node to it's beginning
    *
    * @param node
    *     {@link Node} to be attached
    */
    public <I> void addProducer(Node<I, T> node);

    /**
     * Indicates if the link is the terminal, i.e., there are no nodes appended at the end of this link
     *
     * @return <code>true</code> if there are no nodes
     *         <code>false</code> otherwise
     */
    public boolean isTerminal();
}
