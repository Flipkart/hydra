package flipkart.platform.workflow.queue.events;

import flipkart.platform.workflow.queue.Queue;

/**
* User: shashwat
* Date: 28/07/12
*/
public class QueueAddedEvent<I, Q extends Queue<I>>
{
    public final Q queue;

    public QueueAddedEvent(Q queue)
    {
        this.queue = queue;
    }
}
