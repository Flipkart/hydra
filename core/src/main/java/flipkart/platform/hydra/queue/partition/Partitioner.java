package flipkart.platform.hydra.queue.partition;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.annotations.Beta;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.eventbus.EventBus;
import flipkart.platform.hydra.traits.Subscriber;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.queue.events.QueueAddedEvent;
import flipkart.platform.hydra.queue.events.QueueRemovedEvent;

/**
 * User: shashwat
 * Date: 25/07/12
 */

/**
 * Partitioner is a container of a set of queues and various queue operation policies such as  {@link
 * EnqueuePartitionPolicy} and {@link ReadPartitionPolicy}.
 * Partitioner acts as a repository of available queues on which enqueue and read operations are required to be
 * distributed. It synchronously notifies the registered policies of the changes if they are {@code instanceof} {@link
 * Subscriber}. On receiving the event, the listeners are required to update their internal model if any.
 * <p/>
 * The main idea here is to allow users to pair different (but compatible) partitioning logic for different operations.
 * For example, Partitioner allows users to pair a {@code RandomEnqueuePartitioner} that enqueue messages to a random
 * queue; with a {@code ThreadLocalReadPartitioner} that binds queues to a thread (probably a valid but useless example).
 *
 * @param <I> The payload type
 * @see flipkart.platform.hydra.queue.MessageCtx for generic payload paramter
 * @see flipkart.platform.hydra.queue.ConcurrentPartitionQueue
 */
public class Partitioner<I, Q extends HQueue<I>>
{
    private final Set<Q> queues = Sets.newSetFromMap(new ConcurrentHashMap<Q, Boolean>());
    private final EventBus eventBus = new EventBus(Partitioner.class.getSimpleName());

    public final EnqueuePartitionPolicy<I> enqueuePartitionPolicy;
    public final ReadPartitionPolicy<I> readPartitionPolicy;

    public Partitioner(EnqueuePartitionPolicy<I> enqueuePartitionPolicy,
        ReadPartitionPolicy<I> readPartitionPolicy)
    {
        this.enqueuePartitionPolicy = enqueuePartitionPolicy;
        Subscriber.Utils.register(eventBus, enqueuePartitionPolicy);

        this.readPartitionPolicy = readPartitionPolicy;
        Subscriber.Utils.register(eventBus, readPartitionPolicy);
    }

    public void register(Q queue)
    {
        queues.add(queue);
        eventBus.post(new QueueAddedEvent<I, Q>(queue));
    }

    public void remove(Q queue)
    {
        queues.remove(queue);
        eventBus.post(new QueueRemovedEvent<I, Q>(queue));
    }

    public EnqueuePartitionPolicy<I> getEnqueuePartitionPolicy()
    {
        return enqueuePartitionPolicy;
    }

    public ReadPartitionPolicy<I> getReadPartitionPolicy()
    {
        return readPartitionPolicy;
    }

    /**
     *
     * @return an iterator to a copy of internal queue set
     */
    @Beta
    public UnmodifiableIterator<Q> iterator()
    {
        return Iterators.unmodifiableIterator(new ArrayList<Q>(queues).iterator());
    }
}
