package flipkart.platform.workflow.link;

import flipkart.platform.workflow.node.Node;

/**
 * Link is an interface that defines how two {@link Node}s are connected with
 * each other.
 * 
 * @author shashwat
 * 
 * @param <I>
 */
public interface Link<I>
{
    /**
     * Attach given node to it's end
     * 
     * @param node
     *            {@link Node} to be attached
     */
    public void append(Node<I, ?> node);

    /**
     * Indicates if the link has one or more node to send messages to.
     * 
     * @return <code>true</code> if at minimum one node is available.
     *         <code>false</code> otherwise
     */
    public boolean canForward();

    /**
     * Forward job to the attached nodes
     * 
     * @param i
     *            Job description
     */
    public void forward(I i);

    /**
     * Sends shutdown to the attached nodes.
     * 
     * @param awaitTerminataion
     *            if <code>true</code>, awaits until all nodes are shutdown. Set
     *            to <code>false</code> to return immediately
     * @throws InterruptedException
     */
    public void sendShutdown(boolean awaitTerminataion)
            throws InterruptedException;
}
