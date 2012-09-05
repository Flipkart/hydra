package flipkart.platform.hydra.queue.partition;

import flipkart.platform.hydra.traits.Subscriber;

/**
* User: shashwat
* Date: 27/07/12
*/
public interface EnqueuePartitionPolicy<I> extends Subscriber
{
    void enqueue(I i);
}
