package flipkart.platform.hydra.job;

import flipkart.platform.hydra.node.Node;

/**
 * Interface to define {@link Job} factory. Used in {@link Node}s to create jobs as and when required.
 * 
 * @author shashwat
 * 
 * @param <J>
 *            A {@link Job} type
 */
public interface JobFactory<J>
{
    /**
     * @return a new job
     */
    public J newJob();
}
