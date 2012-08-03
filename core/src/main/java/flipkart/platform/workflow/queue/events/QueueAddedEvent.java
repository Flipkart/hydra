package flipkart.platform.workflow.queue.events;

import flipkart.platform.workflow.queue.HQueue;

/**
* User: shashwat
* Date: 28/07/12
*/
public class QueueAddedEvent<I, Q extends HQueue<I>>
{
    public final Q queue;

    public QueueAddedEvent(Q queue)
    {
        this.queue = queue;
    }
}
