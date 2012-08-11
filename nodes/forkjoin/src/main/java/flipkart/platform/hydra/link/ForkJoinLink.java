package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.traits.CanGroup;
import flipkart.platform.hydra.utils.Pair;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public class ForkJoinLink<I extends CanGroup, O>
{
    private final Predicate<ForkUnit<I, O>> joinPredicate;

    private final ConcurrentMap<String, ForkUnit<I, O>> forkMap = Maps.newConcurrentMap();
    private final ForkLinkImpl forkLink;
    private final JoinLinkImpl joinLink;

    public ForkJoinLink(Predicate<ForkUnit<I, O>> joinPredicate)
    {
        this.joinPredicate = joinPredicate;

        this.forkLink = new ForkLinkImpl();
        this.joinLink = new JoinLinkImpl();
    }
    public ForkJoinLink(Selector<I> forkSelector, Predicate<ForkUnit<I, O>> joinPredicate)
    {
        this.joinPredicate = joinPredicate;

        this.forkLink = new ForkLinkImpl(forkSelector);
        this.joinLink = new JoinLinkImpl();
    }

    @SuppressWarnings("unchecked")
    public void addSource(Node<?, ? extends Collection<I>> node)
    {
        forkLink.addProducer((Node<?,Collection<I>>) node);
    }

    public void addFork(Node<I, Pair<I, O>> node)
    {
        forkLink.addConsumer(node);
        joinLink.addProducer(node);
    }

    public void addConsumer(Node<ForkJoinResult<I, O>, ?> node)
    {
        joinLink.addConsumer(node);
    }

    protected class ForkLinkImpl extends AbstractLink<Collection<I>, I>
    {
        public ForkLinkImpl()
        {
        }

        public ForkLinkImpl(Selector<I> selector)
        {
            super(selector);
        }

        @Override
        protected boolean forward(Node<?, ? extends Collection<I>> collectionNode, Collection<I> collection)
        {
            sendSourceMessages(collection);
            for (I i : collection)
            {
                send(i);
            }
            return true;
        }
    }

    protected class JoinLinkImpl extends AbstractLink<Pair<I, O>, ForkJoinResult<I, O>>
    {
        @Override
        protected boolean forward(Node<?, ? extends Pair<I, O>> pairNode, Pair<I, O> t)
        {
            final ForkJoinResult<I, O> forkJoinResult = joinForkResponse(pairNode, t);
            if (forkJoinResult != null)
            {
                send(forkJoinResult);
                return true;
            }

            return false;
        }
    }

    private void sendSourceMessages(Collection<I> collection)
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
            forkMap.put(entry.getKey(), new ForkUnit<I, O>(entry.getValue(), joinPredicate));
        }
    }

    private ForkJoinResult<I, O> joinForkResponse(Node<?, ? extends Pair<I, O>> pairNode, Pair<I, O> t)
    {
        final ForkUnit<I, O> forkUnit = forkMap.get(t.first.getGroupId());
        if (forkUnit != null && forkUnit.join(t))
        {
            forkMap.remove(t.first.getGroupId());
            return new ForkJoinResult<I, O>(forkUnit.getResult(), forkUnit.getFinishedForks(),
                forkUnit.getUnfinishedForks());
        }

        return null;
    }
}
