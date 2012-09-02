package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.Queues;
import flipkart.platform.hydra.node.AbstractControlNodeEventListener;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.NodeEventListener;
import flipkart.platform.hydra.topology.LinkTopology;
import flipkart.platform.hydra.utils.UnModifiableCollection;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public abstract class AbstractLink<T1, T2> implements GenericLink<T1, T2>
{
    protected final ConcurrentMap<String, Node<T2, ?>> consumerNodes = new ConcurrentHashMap<String, Node<T2, ?>>();
    protected final ConcurrentMap<String, Node<?, T1>> producerNodes = new ConcurrentHashMap<String, Node<?, T1>>();

    private final Queue<LinkEventListener> eventListeners = Queues.newConcurrentLinkedQueue();

    private final Selector<T2> selector;
    private final LinkTopology topology;

    protected AbstractLink(LinkTopology topology)
    {
        this(topology, new DefaultSelector<T2>());
    }

    public AbstractLink(LinkTopology topology, Selector<T2> selector)
    {
        this.topology = topology;
        this.selector = selector;
        topology.addLink(this);
    }

    /*
     * It is essential that {@link #addConsumer(flipkart.platform.hydra.node.Node)} and {@link
     * #addProducer(flipkart.platform.hydra.node.Node)} are synchronized because they add {@link Supervisor} to
     * producers and consumers
     */
    public synchronized <O> void addConsumer(Node<T2, O> node)
    {
        if (validate(node) && consumerNodes.putIfAbsent(node.getIdentity(), node) == null)
        {
            node.addListener(new ConsumerNodeListener<O>());
        }

        for (LinkEventListener eventListener : eventListeners)
        {
            eventListener.onConsumerNodeAdded(this, node);
        }
    }

    @Override
    public UnModifiableCollection<Node<T2, ?>> getConsumers()
    {
        return UnModifiableCollection.from(consumerNodes.values());
    }

    @Override
    public void addEventListener(LinkEventListener listener)
    {
        eventListeners.add(listener);
    }

    /*
    * It is essential that {@link #addConsumer(flipkart.platform.hydra.node.Node)} and {@link
    * #addProducer(flipkart.platform.hydra.node.Node)} are synchronized because they add {@link Supervisor} to
    * producers and consumers
    */
    @Override
    public synchronized <I> void addProducer(Node<I, T1> node)
    {
        if (validate(node) && producerNodes.putIfAbsent(node.getIdentity(), node) == null)
        {
            node.addListener(new ProducerNodeListener());
        }

        for (LinkEventListener eventListener : eventListeners)
        {
            eventListener.onProducerNodeAdded(this, node);
        }
    }

    @Override
    public UnModifiableCollection<Node<?, T1>> getProducers()
    {
        return UnModifiableCollection.from(producerNodes.values());
    }

    @Override
    public LinkTopology getTopology()
    {
        return topology;
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

    protected abstract boolean forward(Node<?, ? extends T1> node, T1 t);

    private void removeConsumer(Node<?, ?> node)
    {
        consumerNodes.remove(node.getIdentity());
        for (LinkEventListener eventListener : eventListeners)
        {
            eventListener.onConsumerNodeRemoved(this, node);
        }

    }

    private void removeProducer(Node<?, ?> node)
    {
        producerNodes.remove(node.getIdentity());
        for (LinkEventListener eventListener : eventListeners)
        {
            eventListener.onProducerNodeRemoved(this, node);
        }
    }

    private synchronized boolean validate(Node node)
    {
        if (node == null)
        {
            throw new IllegalArgumentException("input node cannot be null!");
        }

        return true;
    }

    /**
     *
     */
    private class ProducerNodeListener implements NodeEventListener<T1>
    {
        @Override
        public void onNewMessage(Node<?, ? extends T1> node, T1 t)
        {
            AbstractLink.this.forward(node, t);
        }

        @Override
        public void onShutdown(Node<?, ?> node, boolean awaitTermination) throws InterruptedException
        {
            AbstractLink.this.removeProducer(node);
        }
    }

    /**
     * @param <O>
     */
    private class ConsumerNodeListener<O> extends AbstractControlNodeEventListener<O>
    {
        @Override
        @SuppressWarnings("unchecked")
        public void onShutdown(Node<?, ?> node, boolean awaitTermination) throws InterruptedException
        {
            AbstractLink.this.removeConsumer(node);
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
