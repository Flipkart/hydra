package flipkart.platform.workflow.job;

import flipkart.platform.workflow.job.Job;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.Node;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(I i, Node<I, O> fromNode, Link<O> link);
}
