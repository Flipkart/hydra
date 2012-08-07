package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;

/**
 * Link is an interface that defines how two {@link flipkart.platform.hydra.node.Node}s are connected with
 * each other.
 *
 * @author shashwat
 */
public interface GenericLink<T1, T2>
{
    /**
     * Attach given node to it's end
     *
     * @param node
     *     {@link flipkart.platform.hydra.node.Node} to be attached
     */
    public <O> void addConsumer(Node<T2, O> node);

    /**
    * Attach given node to it's beginning
    *
    * @param node
    *     {@link flipkart.platform.hydra.node.Node} to be attached
    */
    public <I> void addProducer(Node<I, T1> node);

    /**
     * Indicates if the link is the terminal, i.e., there are no nodes appended at the end of this link
     *
     * @return <code>true</code> if there are no nodes
     *         <code>false</code> otherwise
     */
    public boolean isTerminal();
}
