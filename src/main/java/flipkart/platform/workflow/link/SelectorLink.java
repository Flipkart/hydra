package flipkart.platform.workflow.link;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import flipkart.platform.workflow.node.Node;

/**
 * A {@link MultipleLink} that forwards messages to one or more nodes based on a
 * {@link Selector}.
 * 
 * @author shashwat
 * 
 */
public class SelectorLink<T> extends MultipleLink<T>
{
    private final Map<String, Node<T, ?>> readOnlyMap = Collections
            .unmodifiableMap(nodes);

    /**
     * Selector interface to choose one or more nodes
     * 
     * @author shashwat
     * 
     */
    public static interface Selector<T>
    {
        /**
         * Select one or more nodes to forward the message to.
         * 
         * @param i
         *            Job description to be forwarded
         * @param nodes
         *            {@link Node}s registered with this link
         * @return List of nodes to forward to. An empty or null return value
         *         will cause the job to be discarded.
         */
        public List<Node<T, ?>> select(T i, Map<String, Node<T, ?>> nodes);
    }

    private final Selector<T> selector;

    public SelectorLink(SelectorLink.Selector<T> selector)
    {
        this.selector = selector;
    }

    @Override
    public void forward(T i)
    {
        final List<Node<T, ?>> selections = selector.select(i, readOnlyMap);

        if (selections != null)
        {
            for (Node<T, ?> selection : selections)
            {
                selection.accept(i);
            }
        }
    }

    public static <I> SelectorLink<I> create(SelectorLink.Selector<I> selector)
    {
        return new SelectorLink<I>(selector);
    }
}