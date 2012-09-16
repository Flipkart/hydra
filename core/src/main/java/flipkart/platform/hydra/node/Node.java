package flipkart.platform.hydra.node;

import flipkart.platform.hydra.traits.HasIdentity;

/**
 * A {@link flipkart.platform.hydra} node that accepts jobs, executes them and forwards their results
 * to other nodes down the workflow if available.
 * <p/>
 * Provides observers to tap into different node events
 *
 * @param <I>
 *     Input Job description type
 * @param <O>
 *     Output Job description type
 * @author shashwat
 * @see NodeEventListener
 * @see flipkart.platform.hydra.link.Link
 * @see BaseNode
 */
public interface Node<I, O> extends HasIdentity
{
    /**
     * @param nodeListener
     *     {@link NodeEventListener} to be added to the list of listeners
     */
    public void addListener(NodeEventListener<O> nodeListener);

    /**
     * Accept a job description that needs to be processed by this node.
     *
     * @param i
     *     job description
     */
    public void accept(I i);

    /**
     * Terminate this node and all other attached nodes. No more jobs will be
     * accepted after node is shutdown. Method may not be called multiple times.
     * Has weak thread safety guarantees.
     *
     * @param awaitTermination
     *     <code>true</code> to wait for termination of all nodes.
     *     <code>false</code> otherwise.
     * @throws InterruptedException
     *     if the thread is interrupted
     */
    public void shutdown(boolean awaitTermination) throws InterruptedException;

    /**
     * @return <code>true</code> if this node is shutdown, <code>false</code> otherwise
     */
    public boolean isShutdown();
}
