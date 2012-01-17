package flipkart.platform.workflow.job;

import flipkart.platform.workflow.node.Node;

import java.util.Map;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(I i, Node<I, O> fromNode, Map<String, Node<O, ?>> nodes)
            throws ExecutionFailureException;
}
