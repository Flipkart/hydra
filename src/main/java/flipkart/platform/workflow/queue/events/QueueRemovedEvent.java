package flipkart.platform.workflow.queue.events;

import flipkart.platform.workflow.queue.Queue;

/**
* User: shashwat
* Date: 28/07/12
*/
public class QueueRemovedEvent<I, Q extends Queue<I>>
{
    public final Q queue;

    public QueueRemovedEvent(Q queue)
    {
        this.queue = queue;
    }
}
