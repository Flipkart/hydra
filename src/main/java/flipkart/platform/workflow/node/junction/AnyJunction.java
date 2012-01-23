package flipkart.platform.workflow.node.junction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;
import flipkart.platform.workflow.node.TypeMismatchException;

public class AnyJunction
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
         * 
         */
        public void select(AnyNode<?, T> node, T i,
                Map<String, AnyNode<?, ?>> fromNodes,
                Map<String, AnyNode<?, ?>> toNodes);
    }

    public class FromBuilder<O>
    {
        private final Selector<O> selector;

        private FromBuilder(Selector<O> selector)
        {
            this.selector = selector;
        }

        public void bind(AnyNode<?, O> node) throws TypeMismatchException
        {
            from(selector, node);
        }
    }

    public static enum Isolation
    {
        /**
         * Forward shutdown signal to toNodes, if any of fromNodes sends it.
         */
        NONE,
        /**
         * Forward shutdown signal to toNodes, only if all fromNodes sends it.
         */
        REF_COUNTED,
        /**
         * Never forward shutdown to toNodes.
         */
        ISOLATE_WORKFLOW
    }

    private final Map<String, AnyNode<?, ?>> fromNodes = new ConcurrentHashMap<String, AnyNode<?, ?>>();
    private final Map<String, AnyNode<?, ?>> toNodes = new ConcurrentHashMap<String, AnyNode<?, ?>>();

    private final AtomicInteger shutdown = new AtomicInteger(0);

    private final Isolation level;

    private final String name;

    public AnyJunction(String name)
    {
        this(name, Isolation.NONE);
    }

    public AnyJunction(String name, Isolation level)
    {
        this.name = name;
        this.level = level;
    }

    public <T> FromBuilder<T> from(Selector<T> selector)
    {
        return new FromBuilder<T>(selector);
    }

    public <T> void from(Selector<T> selector, AnyNode<?, T> fromNode)
            throws TypeMismatchException
    {
        fromNode.appendAny(new AnyJunctionNode<T>(fromNode, selector).anyNode());
        fromNodes.put(fromNode.getName(), fromNode);
        shutdown.incrementAndGet();
    }

    public <I> void to(AnyNode<I, ?> toNode)
    {
        toNodes.put(toNode.getName(), toNode);
    }

    private class AnyJunctionNode<T> implements Node<T, T>
    {
        private final AnyNode<?, T> fromNode;
        private final Selector<T> selector;

        public AnyJunctionNode(AnyNode<?, T> fromNode, Selector<T> selector)
        {
            this.fromNode = fromNode;
            this.selector = selector;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public void accept(T i)
        {
            selector.select(fromNode, i, fromNodes, toNodes);
        }

        public AnyNode<T, T> anyNode()
        {
            return new AnyNode<T, T>(this);
        }

        @Override
        public void append(Node<T, ?> node)
        {
            throw new UnsupportedOperationException(
                    "Append to this node is not supported. Trying to append node: "
                            + node.getName());
        }

        @Override
        public void shutdown(boolean awaitTermination)
                throws InterruptedException
        {
            AnyJunction.this.shutdown(awaitTermination);
        }
    }

    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        final boolean canShutDown;
        switch (level)
        {
        case NONE:
            int count = shutdown.get();
            canShutDown = count > 0 && shutdown.compareAndSet(count, 0);
            break;
        case REF_COUNTED:
            canShutDown = (0 == shutdown.decrementAndGet());
            break;
        case ISOLATE_WORKFLOW:
        default:
            canShutDown = false;
            break;
        }

        if (canShutDown)
        {
            for (AnyNode<?, ?> node : toNodes.values())
            {
                node.shutdown(awaitTermination);
            }
        }
    }
}
