package flipkart.platform.workflow.link;

import flipkart.platform.workflow.node.Node;

/**
 * Link is an interface that defines how two {@link Node}s are connected with
 * each other.
 *
 * @param <I>
 * @author shashwat
 */
public interface Link<I>
{
    /**
     * Attach given node to it's end
     *
     * @param node
     *     {@link Node} to be attached
     */
    public void append(Node<I, ?> node);

    /**
     * Indicates if the link is the terminal, i.e., there are no nodes appended at the end of this link
     *
     * @return <code>true</code> if there are no nodes
     *         <code>false</code> otherwise
     */
    public boolean isTerminal();

    /**
     * Forward job to the attached nodes
     *
     * @param i
     *     Job description
     * @return <code>true</code>, if the message was actually forwarded to a node;
     *         <code>false</code> otherwise
     *
     */
    public boolean forward(I i);

    /**
     * Sends shutdown to the attached nodes.
     *
     * @param awaitTermination
     *     if <code>true</code>, awaits until all nodes are shutdown. Set
     *     to <code>false</code> to return immediately
     * @throws InterruptedException if thread is interrupted
     */
    public void sendShutdown(boolean awaitTermination) throws InterruptedException;
}
