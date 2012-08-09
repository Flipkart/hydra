package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.node.AbstractMessageEventListener;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.traits.CanGroup;
import flipkart.platform.hydra.utils.Pair;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public class JoinLink<I extends CanGroup, S> extends AbstractLink<Pair<I, S>, UnModifiableMap<I, S>>
{
    public final ConcurrentMap<String, ForkUnit<I, S>> map = new ConcurrentHashMap<String, ForkUnit<I, S>>();

    public JoinLink(Node<?, Collection<I>> forkNode, JoinPredicate<I, S> joinPredicate)
    {
        forkNode.addListener(new ForkEventListener(joinPredicate));
    }

    @Override
    protected boolean forward(Pair<I, S> t)
    {
        final ForkUnit<I, S> forkUnit = map.get(t.first.getGroupId());
        if (forkUnit.join(t))
        {
            send(forkUnit.getFinishedForks());
        }

        return true;
    }

    /**
     * Listens to Fork node output messages and creates batches of messages grouped by GroupId that needs to be
     * processed concurrently and later joined once the join predicate returns true
     */
    private class ForkEventListener extends AbstractMessageEventListener<Collection<I>>
    {
        private final JoinPredicate<I, S> joinPredicate;

        public ForkEventListener(JoinPredicate<I, S> joinPredicate)
        {
            this.joinPredicate = joinPredicate;
        }

        @Override
        public void onNewMessage(Collection<I> collection)
        {
            final Map<String, Collection<I>> partitionCollection = Maps.newHashMap();
            for (I i : collection)
            {
                Collection<I> col = partitionCollection.get(i.getGroupId());
                if (col == null)
                {
                    col = Lists.newLinkedList();
                    partitionCollection.put(i.getGroupId(), col);
                }
                col.add(i);
            }
            for (Map.Entry<String, Collection<I>> entry : partitionCollection.entrySet())
            {
                map.put(entry.getKey(), new ForkUnit<I, S>(entry.getValue(), joinPredicate));
            }
        }
    }
}
