package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.NodeEventListener;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public abstract class AbstractLink<T1, T2> implements GenericLink<T1, T2>
{
    protected final ConcurrentMap<String, Node<T2, ?>> consumerNodes = new ConcurrentHashMap<String, Node<T2, ?>>();
    protected final ConcurrentMap<String, Node<?, T1>> producerNodes = new ConcurrentHashMap<String, Node<?, T1>>();

    private final Selector<T2> selector;

    protected AbstractLink()
    {
        this(new DefaultSelector<T2>());
    }

    public AbstractLink(Selector<T2> selector)
    {
        this.selector = selector;
    }

    private enum RunState
    {
        ACTIVE, SHUTTING_DOWN, SHUTDOWN
    }

    private volatile RunState runState = RunState.ACTIVE;

    public <O> void addConsumer(Node<T2, O> node)
    {
        if (valid(node))
        {
            if (consumerNodes.putIfAbsent(node.getName(), node) == null)
                node.addListener(new ConsumerNodeListener<O>());
        }
    }

    @Override
    public <I> void addProducer(Node<I, T1> node)
    {
        if (valid(node))
        {
            if (producerNodes.putIfAbsent(node.getName(), node) == null)
                node.addListener(new ProducerNodeListener());
        }
    }

    public boolean send(T2 i)
    {
        final Collection<Node<T2, ?>> selectedNodes = selector.select(i, UnModifiableMap.from(consumerNodes));
        
        if (selectedNodes != null)
        {
            int count = 0;
            for (Node<T2, ?> selection : selectedNodes)
            {
                if (selection != null)
                {
                    ++count;
                    selection.accept(i);
                }
            }
            return (count > 0);
        }

        return false;
    }


    @Override
    public boolean isTerminal()
    {
        return consumerNodes.isEmpty();
    }

    protected synchronized void tryShutdown(boolean awaitTermination) throws InterruptedException
    {
        if (runState == RunState.ACTIVE && producerNodes.isEmpty())
        {
            runState = RunState.SHUTTING_DOWN;

            for (Node<?, ?> node : consumerNodes.values())
            {
                node.shutdown(awaitTermination);
            }
            runState = RunState.SHUTDOWN;
        }
    }

    protected abstract boolean forward(T1 t);

    private void removeConsumer(Node<T2, ?> node)
    {
        consumerNodes.remove(node.getName());
    }

    private void removeProducer(Node<?, T1> node)
    {
        producerNodes.remove(node.getName());
    }

    private synchronized boolean valid(Node<?, ?> node)
    {
        if (node == null)
        {
            throw new IllegalArgumentException("input node cannot be null!");
        }

        if (runState != RunState.ACTIVE)
        {
            throw new IllegalStateException("Link is already shutting down. Cannot made modifications");
        }

        return true;
    }

    /**
     *
     */
    private class ProducerNodeListener implements NodeEventListener<T1>
    {
        @Override
        public void onNewMessage(T1 t)
        {
            AbstractLink.this.forward(t);
        }

        @Override
        public void onShutdown(Node<?, T1> node, boolean awaitTermination) throws InterruptedException
        {
            AbstractLink.this.removeProducer(node);
            AbstractLink.this.tryShutdown(awaitTermination);
        }
    }

    /**
     * @param <O>
     */
    private class ConsumerNodeListener<O> implements NodeEventListener<O>
    {
        @Override
        public void onNewMessage(O o)
        {
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onShutdown(Node<?, O> node, boolean awaitTermination) throws InterruptedException
        {
            AbstractLink.this.removeConsumer((Node<T2, ?>) node);
        }
    }

    private static class DefaultSelector<T> implements Selector<T>
    {
        @Override
        public Collection<Node<T, ?>> select(T i, UnModifiableMap<String, Node<T, ?>> nodes)
        {
            return nodes.values();
        }
    }
}
