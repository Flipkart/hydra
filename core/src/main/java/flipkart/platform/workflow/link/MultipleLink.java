package flipkart.platform.workflow.link;

import java.util.HashMap;
import java.util.Map;

import flipkart.platform.workflow.node.Node;

/**
 * An abstract super class to all {@link Link}s that can have more than one
 * attached nodes and can forward messages to one or more nodes.
 * 
 * @author shashwat
 * 
 */
abstract class MultipleLink<T> implements Link<T>
{
    protected final Map<String, Node<T, ?>> nodes = new HashMap<String, Node<T, ?>>();

    public void append(Node<T, ?> node)
    {
        nodes.put(node.getName(), node);
    }

    public boolean canForward()
    {
        return !nodes.isEmpty();
    }

    public abstract void forward(T i);

    public void sendShutdown(boolean awaitTermination)
            throws InterruptedException
    {
        for (Node<T, ?> node : nodes.values())
        {
            node.shutdown(awaitTermination);
        }
    }
}
