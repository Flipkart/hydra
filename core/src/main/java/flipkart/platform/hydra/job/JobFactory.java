package flipkart.platform.hydra.job;

import flipkart.platform.hydra.node.Node;

/**
 * Interface to define {@link flipkart.platform.hydra.traits.Initializable} object factory. Used in workflow
 * {@link Node}s to create jobs as and when required.
 * 
 * @author shashwat
 * 
 * @param <J>
 *            A {@link Job} type
 */
public interface JobFactory<J>
{
    public J newJob();
}
