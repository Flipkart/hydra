package flipkart.platform.workflow.node.junction;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;

abstract class AbstractJunction
{
    protected static AtomicInteger junctionCounter = new AtomicInteger();

    protected final AtomicBoolean shutdown = new AtomicBoolean(false);
    protected final AtomicInteger junctionNodeCounter = new AtomicInteger();
    protected final String name = "junction-"
            + junctionCounter.incrementAndGet();

    abstract public void shutdown(boolean awaitTerminataion)
            throws InterruptedException;

    abstract class AbstractJunctionNode<I> implements Node<I, I>
    {
        protected final String juncName;

        public AbstractJunctionNode(Node<?, I> parentNode)
        {
            this.juncName = name + "-" + parentNode.getName() + "-"
                    + junctionNodeCounter.incrementAndGet();
        }

        public AbstractJunctionNode(AnyNode<?, I> parentNode)
        {
            this.juncName = name + "-" + parentNode.getName() + "-"
                    + junctionNodeCounter.incrementAndGet();
        }

        @Override
        public String getName()
        {
            return juncName;
        }

        @Override
        public void append(Node<I, ?> node)
        {
            throw new UnsupportedOperationException(
                    "Append to this node is not supported. Trying to append node: "
                            + node.getName());
        }

        @Override
        public void shutdown(boolean awaitTerminataion)
                throws InterruptedException
        {
            AbstractJunction.this.shutdown(awaitTerminataion);
        }

    }
}
