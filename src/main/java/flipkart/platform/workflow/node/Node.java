package flipkart.platform.workflow.node;

import flipkart.platform.workflow.link.SelectorLink;

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
     * to this node (as in the case of {@link SelectorLink}).
     * 
     * @return Node name
     */
    public String getName();

    /**
     * Append given node to this node. Once a node is appended, it cannot be
     * detached. Also, no nodes should be appended once the execution has
     * started. Has weak thread safety guarantees.
     * 
     * @param node
     *            {@link Node} to append
     */
    public void append(Node<O, ?> node);

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
     * @param awaitTerminataion
     *            <code>true</code> to wait for termination of all nodes.
     *            <code>false</code> otherwise.
     * @throws InterruptedException
     */
    public void shutdown(boolean awaitTerminataion) throws InterruptedException;
}
