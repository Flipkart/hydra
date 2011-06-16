package flipkart.platform.workflow.link;

import flipkart.platform.workflow.node.Node;

/**
 * A {@link MultipleLink} that forwards messages to all attached nodes
 * 
 * @author shashwat
 * 
 */
public class BroadcastLink<T> extends MultipleLink<T>
{
    @Override
    public void forward(T i)
    {
        for (final Node<T, ?> stationNode : nodes.values())
        {
            stationNode.accept(i);
        }
    }

    public static <I> BroadcastLink<I> create()
    {
        return new BroadcastLink<I>();
    }

}