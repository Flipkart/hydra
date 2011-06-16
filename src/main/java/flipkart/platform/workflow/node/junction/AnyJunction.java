package flipkart.platform.workflow.node.junction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.TypeMismatchException;

public class AnyJunction extends AbstractJunction
{
    public static interface Selector<T>
    {
        /**
         * /* Select one or more nodes to forward the message to.
         * 
         * @param node
         *            {@link Node} that forwarded the message
         * @param i
         *            Job description to be forwarded
         * @param fromNodes
         *            {@link Node}s containing this link
         * @param toNodes
         *            {@link Node}s registered with this link
         * @return List of nodes to forward to. An empty or null return value
         *         will cause the job to be discarded.
         * 
         */
        public void select(AnyNode<?, T> node, T i,
                Map<String, AnyNode<?, ?>> fromNodes,
                Map<String, AnyNode<?, ?>> toNodes);
    }

    public class FromBuilder<O>
    {
        private final Selector<O> selector;

        public FromBuilder(Selector<O> selector)
        {
            this.selector = selector;
        }

        public void bind(AnyNode<?, O> node) throws TypeMismatchException
        {
            from(selector, node);
        }
    }

    private final Map<String, AnyNode<?, ?>> fromNodes = new ConcurrentHashMap<String, AnyNode<?, ?>>();
    private final Map<String, AnyNode<?, ?>> toNodes = new ConcurrentHashMap<String, AnyNode<?, ?>>();

    public <T> FromBuilder<T> from(Selector<T> selector)
    {
        return new FromBuilder<T>(selector);
    }

    public <T> void from(Selector<T> selector, AnyNode<?, T> fromNode)
            throws TypeMismatchException
    {
        fromNode.appendAny(new AnyJunctionNode<T>(fromNode, selector).anyNode());
        fromNodes.put(fromNode.getName(), fromNode);
    }

    public <I> void to(AnyNode<I, ?> toNode)
    {
        toNodes.put(toNode.getName(), toNode);
    }

    private class AnyJunctionNode<T> extends AbstractJunctionNode<T>
    {
        private final AnyNode<?, T> parentNode;
        private final Selector<T> selector;

        public AnyJunctionNode(AnyNode<?, T> parentNode, Selector<T> selector)
        {
            super(parentNode);
            this.parentNode = parentNode;
            this.selector = selector;
        }

        @Override
        public void accept(T i)
        {
            selector.select(parentNode, i, fromNodes, toNodes);
        }

        public AnyNode<T, T> anyNode()
        {
            return new AnyNode<T, T>(this);
        }
    }

    @Override
    public void shutdown(boolean awaitTerminataion) throws InterruptedException
    {
        if (shutdown.compareAndSet(false, true))
        {
            for (AnyNode<?, ?> node : toNodes.values())
            {
                node.shutdown(awaitTerminataion);
            }
        }
    }
}
