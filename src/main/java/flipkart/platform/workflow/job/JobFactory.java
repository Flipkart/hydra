package flipkart.platform.workflow.job;

import flipkart.platform.workflow.node.Node;

/**
 * Interface to define {@link Initializable} object factory. Used in workflow
 * {@link Node}s to create jobs as and when required.
 * 
 * @author shashwat
 * 
 * @param <J>
 *            An {@link Initializable} type
 */
public interface JobFactory<J extends Initializable>
{
    public J newJob();
}
