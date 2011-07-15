package flipkart.platform.workflow.job;

import java.util.Map;

import flipkart.platform.workflow.node.Node;

public interface BasicJob<I, O> extends Job<I>
{
    public void execute(I i, Node<I, O> fromNode, Map<String, Node<O, ?>> nodes)
            throws ExecutionFailureException;
}
