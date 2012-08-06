package flipkart.platform.hydra.queue;

import com.google.common.collect.UnmodifiableIterator;
import flipkart.platform.hydra.queue.partition.EnqueuePartitionPolicy;
import flipkart.platform.hydra.queue.partition.Partitioner;
import flipkart.platform.hydra.queue.partition.ReadPartitionPolicy;
import flipkart.platform.hydra.utils.RefCounter;

/**
 * User: shashwat
 * Date: 25/07/12
 */

/**
 * {@link HQueue} implementation that can partition messages over a given set of queues using a {@link Partitioner}.
 *
 * @param <I>
 * @param <Q>
 */
public class ConcurrentPartitionQueue<I, Q extends HQueue<I>> implements HQueue<I>
{
    private final Partitioner<I, Q> partitioner;
    private final RefCounter sizeCounter = new RefCounter(0);

    public ConcurrentPartitionQueue(Partitioner<I, Q> partitioner)
    {
        this.partitioner = partitioner;
    }

    /**
     * A factory method to create a new {@link ConcurrentPartitionQueue} with a given number of partitions of a queue
     * that implements {@link HQueue} and different operation-specific partition policies
     *
     * @param numPartitions int, number of queues to create
     * @param queueFactory {@link QueueFactory} instance to create new queue for each partition
     * @param enqueuePartitionPolicy {@link EnqueuePartitionPolicy} partition policy to enqueue new messages
     * @param readPartitionPolicy {@link ReadPartitionPolicy} partition policy to read messages from queue
     * @param <I> Message payload type
     * @param <Q> HQueue type
     * @return new {@link ConcurrentPartitionQueue} instance
     */
    public static <I, Q extends HQueue<I>> ConcurrentPartitionQueue<I, Q> create(int numPartitions,
        QueueFactory<I, Q> queueFactory, EnqueuePartitionPolicy<I> enqueuePartitionPolicy,
        ReadPartitionPolicy<I> readPartitionPolicy)
    {
        final Partitioner<I, Q> partitioner = new Partitioner<I, Q>(enqueuePartitionPolicy, readPartitionPolicy);
        for (int i = 0; i < numPartitions; ++i)
        {
            partitioner.register(queueFactory.newQueue());
        }
        return new ConcurrentPartitionQueue<I, Q>(partitioner);
    }

    @Override
    public void enqueue(I i)
    {
        partitioner.getEnqueuePartitionPolicy().enqueue(i);
        sizeCounter.offer();
    }

    @Override
    public MessageCtx<I> read()
    {
        final MessageCtx<I> messageCtx = partitioner.getReadPartitionPolicy().read();
        sizeCounter.take();
        return messageCtx;
    }

    @Override
    public MessageCtxBatch<I> read(int limit)
    {
        final MessageCtxBatch<I> messageCtxBatch = partitioner.getReadPartitionPolicy().read(limit);
        sizeCounter.take(messageCtxBatch.size());
        return messageCtxBatch;
    }

    public UnmodifiableIterator<? extends HQueue<I>> iterator()
    {
        return partitioner.iterator();
    }

    @Override
    public int size()
    {
        return (int) sizeCounter.peek();
    }

    @Override
    public boolean isEmpty()
    {
        return sizeCounter.isZero();
    }
}
