package flipkart.platform.hydra.queue.partition;

/**
* User: shashwat
* Date: 27/07/12
*/
public interface EnqueuePartitionPolicy<I>
{
    void enqueue(I i);
}
