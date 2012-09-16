package flipkart.platform.hydra.link;

import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.topology.LinkTopology;
import flipkart.platform.hydra.utils.UnModifiableCollection;

/**
 * Link is an interface that defines how two or more {@link flipkart.platform.hydra.node.Node}s are connected
 * with each other. In other words, links defines and controls the data flow between nodes. All instances must be part
 * of one and only one {@link flipkart.platform.hydra.topology.LinkTopology}
 * <p/>
 * Link can chose to transform the output (of type {@link T1}) of the producers to type {@link T2} as input type
 * to consumers.
 *
 * @param <T1>
 *     output type generated from producers
 * @param <T2>
 *     input type expected by consumers
 * @author shashwat
 * @see DefaultLink
 * @see LinkTopology
 */
public interface Link<T1, T2>
{
    /**
     * Add a consumer {@link Node}. Consumers will receive events of type {@link T2}.
     *
     * @param node
     *     {@link flipkart.platform.hydra.node.Node} to be added as consumer
     */
    public <O> void addConsumer(Node<T2, O> node);

    /**
     * Add a producer {@link Node}. Producers will generate events of type {@link T1}.
     *
     * @param node
     *     {@link flipkart.platform.hydra.node.Node} to be added as producer
     */
    public <I> void addProducer(Node<I, T1> node);

    /**
     * @return LinkTopology The topology to which this link belong
     */
    LinkTopology getTopology();

    /**
     * Send a message to consumers
     *
     * @param t2 {@link T2}
     *     message that needs to be sent
     * @return true only if message was sent to at least one consumer
     */
    public boolean send(T2 t2);

    /**
     * Indicates if the link is the terminal, i.e., there are no consumers added to this link
     *
     * @return <code>true</code> if there are no consumers
     *         <code>false</code> otherwise
     */
    public boolean isTerminal();

    /**
     * @return {@link UnModifiableCollection} of the producer {@link Node}s.
     */
    public UnModifiableCollection<Node<?, T1>> getProducers();

    /**
     * @return {@link UnModifiableCollection} of the consumer {@link Node}s
     */
    public UnModifiableCollection<Node<T2, ?>> getConsumers();

    /**
     * Add a {@link LinkEventListener} to this link.  
     * @param listener {@link LinkEventListener} to be added.
     */
    public void addEventListener(LinkEventListener listener);
}
