package flipkart.platform.hydra.link;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.NodeEventListener;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public abstract class AbstractLink<T> implements Link<T>
{
    protected final ConcurrentMap<String, Node<T, ?>> consumerNodes = new ConcurrentHashMap<String, Node<T, ?>>();
    protected final ConcurrentMap<String, Node<?, T>> producerNodes = new ConcurrentHashMap<String, Node<?, T>>();

    private enum RunState
    {
        ACTIVE, SHUTTING_DOWN, SHUTDOWN
    }

    private volatile RunState runState = RunState.ACTIVE;

    public <O> void addConsumer(Node<T, O> node)
    {
        if (valid(node))
        {
            if (consumerNodes.putIfAbsent(node.getName(), node) == null)
                node.addListener(new ConsumerNodeListener<O>());
        }
    }

    @Override
    public <I> void addProducer(Node<I, T> node)
    {
        if (valid(node))
        {
            if (producerNodes.putIfAbsent(node.getName(), node) == null)
                node.addListener(new ProducerNodeListener());
        }
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

            for (Node<T, ?> node : consumerNodes.values())
            {
                node.shutdown(awaitTermination);
            }
            runState = RunState.SHUTDOWN;
        }
    }

    protected abstract boolean forward(T t);

    private void removeConsumer(Node<T, ?> node)
    {
        consumerNodes.remove(node.getName());
    }

    private void removeProducer(Node<?, T> node)
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
    private class ProducerNodeListener implements NodeEventListener<T>
    {
        @Override
        public void forward(T t)
        {
            AbstractLink.this.forward(t);
        }

        @Override
        public void onShutdown(Node<?, T> node, boolean awaitTermination) throws InterruptedException
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
        public void forward(O o)
        {
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onShutdown(Node<?, O> node, boolean awaitTermination) throws InterruptedException
        {
            AbstractLink.this.removeConsumer((Node<T, ?>) node);
        }
    }

}
