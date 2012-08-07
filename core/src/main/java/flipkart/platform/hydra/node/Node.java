package flipkart.platform.hydra.node;

import flipkart.platform.hydra.link.Link;

/**
 * A workflow node that accepts jobs, executes them and forwards their results
 * to other nodes down the workflow if available.
 * 
 * @author shashwat
 * 
 * @param <I>
 *            Input Job description type
 * @param <O>
 *            Output Job description type
 */
public interface Node<I, O>
{
    /**
     * Get node name. Is used to identify a node if multiple nodes are attached
     * to this node.
     * 
     * @return Node name
     */
    public String getName();

    public void addListener(NodeEventListener<O> nodeListener);

    /**
     * Accept a job description that will be eventually executed.
     * 
     * @param i
     *            job description
     */
    public void accept(I i);

    /**
     * Terminate this node and all other attached nodes. No more jobs will be
     * accepted after node is shutdown. Method may not be called multiple times.
     * Has weak thread safety guarantees.
     * 
     * @param awaitTermination
     *            <code>true</code> to wait for termination of all nodes.
     *            <code>false</code> otherwise.
     * @throws InterruptedException if the thread is interrupted
     */
    public void shutdown(boolean awaitTermination) throws InterruptedException;
}
