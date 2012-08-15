package flipkart.platform.hydra.link;

import java.util.Queue;
import flipkart.platform.hydra.node.AbstractNodeBase;
import flipkart.platform.hydra.queue.HQueue;

/**
* User: shashwat
* Date: 11/08/12
*/
public class ResultNode<O> extends AbstractNodeBase<O, Void>
{
    private final Queue<O> queue;

    public ResultNode(String name, Queue<O> queue)
    {
        super(name);
        this.queue = queue;
    }

    @Override
    protected void acceptMessage(O result)
    {
        queue.add(result);
    }

    @Override
    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        System.out.println("Shutdown complete!");
    }
}
