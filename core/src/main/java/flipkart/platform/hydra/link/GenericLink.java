package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.topology.LinkTopology;
import flipkart.platform.hydra.utils.UnModifiableCollection;

/**
 * Link is an interface that defines how two {@link flipkart.platform.hydra.node.Node}s are connected with
 * each other.
 *
 * @author shashwat
 */
public interface GenericLink<T1, T2>
{
    /**
     * Attach given node to it's end
     *
     * @param node
     *     {@link flipkart.platform.hydra.node.Node} to be attached
     */
    public <O> void addConsumer(Node<T2, O> node);

    /**
     * Attach given node to it's beginning
     *
     * @param node
     *     {@link flipkart.platform.hydra.node.Node} to be attached
     */
    public <I> void addProducer(Node<I, T1> node);

    LinkTopology getTopology();

    /**
     * Send a message to consumers
     *
     * @param t2
     *     message that needs to be sent
     * @return true only if message was sent to at least one consumer
     */
    public boolean send(T2 t2);

    /**
     * Indicates if the link is the terminal, i.e., there are no nodes appended at the end of this link
     *
     * @return <code>true</code> if there are no nodes
     *         <code>false</code> otherwise
     */
    public boolean isTerminal();

    public UnModifiableCollection<Node<?, T1>> getProducers();

    public UnModifiableCollection<Node<T2, ?>> getConsumers();

    public void addEventListener(LinkEventListener listener);
}
