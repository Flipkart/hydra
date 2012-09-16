package flipkart.platform.hydra.queue.events;

import flipkart.platform.hydra.queue.HQueue;

/**
* User: shashwat
* Date: 28/07/12
*/
public class QueueRemovedEvent<I, Q extends HQueue<I>>
{
    public final Q queue;

    public QueueRemovedEvent(Q queue)
    {
        this.queue = queue;
    }
}
