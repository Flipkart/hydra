package flipkart.platform.hydra.queue;

/**
 * User: shashwat
 * Date: 28/07/12
 */
public interface QueueFactory<I, Q extends HQueue<I>>
{
    Q newQueue();
}
