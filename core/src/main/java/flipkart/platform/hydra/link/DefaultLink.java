package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * A class that implements default {@link Link} that can have more than one attached nodes and can forward messages to
 * one or more nodes depending if a {@link Selector}. Default behavior is to send messages to all the nodes.
 *
 * @author shashwat
 */
public class DefaultLink<T> implements Link<T>
{
    protected final Map<String, Node<T, ?>> nodes = new ConcurrentHashMap<String, Node<T, ?>>();
    private final Selector<T> selector;

    public static <T> DefaultLink<T> from(Selector<T> selector)
    {
        return new DefaultLink<T>(selector);
    }
    
    public DefaultLink()
    {
        this(new DefaultSelector<T>());
    }

    public DefaultLink(Selector<T> selector)
    {
        this.selector = selector;
    }

    public void append(Node<T, ?> node)
    {
        if (node == null)
        {
            throw new IllegalArgumentException("input cannot be null!");
        }
        nodes.put(node.getName(), node);
    }

    @Override
    public boolean isTerminal()
    {
        return nodes.isEmpty();
    }

    public boolean forward(T i)
    {
        int count = 0;
        final Collection<Node<T, ?>> selectedNodes = selector.select(i, UnModifiableMap.from(nodes));

        if (selectedNodes != null && !selectedNodes.isEmpty())
        {
            for (Node<T, ?> selection : selectedNodes)
            {
                if (selection != null)
                {
                    ++count;
                    selection.accept(i);
                }
            }
        }

        return (count > 0);
    }

    public void sendShutdown(boolean awaitTermination)
        throws InterruptedException
    {
        for (Node<T, ?> node : nodes.values())
        {
            node.shutdown(awaitTermination);
        }
    }

    private static class DefaultSelector<T> implements Selector<T>
    {
        @Override
        public Collection<Node<T, ?>> select(T i, UnModifiableMap<String, Node<T, ?>> nodes)
        {
            return nodes.values();
        }
    }
}
