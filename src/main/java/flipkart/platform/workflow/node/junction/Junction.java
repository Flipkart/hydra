package flipkart.platform.workflow.node.junction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import flipkart.platform.workflow.node.Node;

public class Junction<I> extends AbstractJunction
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
        public List<Node<T, ?>> select(Node<?, T> node, T i,
                Map<String, Node<?, T>> fromNodes,
                Map<String, Node<T, ?>> toNodes);
    }

    private final Map<String, Node<?, I>> fromNodes = new ConcurrentHashMap<String, Node<?, I>>();
    private final Map<String, Node<I, ?>> toNodes = new ConcurrentHashMap<String, Node<I, ?>>();
    private final Selector<I> selector;

    public Junction(Selector<I> selector)
    {
        this.selector = selector;
    }

    public void from(Node<?, I> fromNode)
    {
        fromNode.append(new JunctionNode(fromNode));
        fromNodes.put(fromNode.getName(), fromNode);
    }

    public void to(Node<I, ?> toNode)
    {
        toNodes.put(toNode.getName(), toNode);
    }

    @SuppressWarnings("unchecked")
    public <T1, T2> void fromAny(Node<T1, T2> node)
    {
        from((Node<?, I>) node);
    }

    @SuppressWarnings("unchecked")
    public <T1, T2> void toAny(Node<T1, T2> node)
    {
        to((Node<I, ?>) node);
    }

    public static <T> Junction<T> create(Selector<T> selector)
    {
        return new Junction<T>(selector);
    }

    private class JunctionNode extends AbstractJunctionNode<I>
    {
        private final Node<?, I> parentNode;

        public JunctionNode(Node<?, I> parentNode)
        {
            super(parentNode);
            this.parentNode = parentNode;
        }

        @Override
        public void accept(I i)
        {
            final List<Node<I, ?>> nodes = selector.select(parentNode, i,
                    fromNodes, toNodes);
            if (nodes != null)
            {
                for (Node<I, ?> node : nodes)
                {
                    node.accept(i);
                }
            }
        }
    }

    @Override
    public void shutdown(boolean awaitTerminataion) throws InterruptedException
    {
        if (shutdown.compareAndSet(false, true))
        {
            for (Node<I, ?> node : toNodes.values())
            {
                node.shutdown(awaitTerminataion);
            }
        }
    }
}
