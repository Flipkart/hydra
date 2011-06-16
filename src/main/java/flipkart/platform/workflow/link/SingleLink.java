package flipkart.platform.workflow.link;

import flipkart.platform.workflow.node.Node;

/**
 * A simple {@link Link} that allows only one node to be attached to it's end.
 * Attempting to append multiple nodes will cause older node to be replaced.
 * 
 * @author shashwat
 * 
 */
public class SingleLink<T> implements Link<T>
{
    private Node<T, ?> node = null;

    public void append(Node<T, ?> node)
    {
        this.node = node;
    }

    public boolean canForward()
    {
        return node != null;
    }

    public void forward(T i)
    {
        if (canForward())
        {
            node.accept(i);
        }
    }

    public void sendShutdown(boolean awaitTerminataion)
            throws InterruptedException
    {
        if (canForward())
        {
            node.shutdown(awaitTerminataion);
        }
    }

    public static <I> SingleLink<I> create()
    {
        return new SingleLink<I>();
    }
}
