package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.traits.CanGroup;
import flipkart.platform.hydra.utils.Pair;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public class NodeJoinLink<I extends CanGroup, O>
{
    private final Predicate<ForkUnit<String, O>> joinPredicate;
    private final ConcurrentMap<String, Pair<I, ForkUnit<String, O>>> forkMap = Maps.newConcurrentMap();

    private final ForkLinkImpl forkLink;
    private final JoinLinkImpl joinLink;

    public NodeJoinLink(Predicate<ForkUnit<String, O>> joinPredicate)
    {
        this.joinPredicate = joinPredicate;

        this.forkLink = new ForkLinkImpl();
        this.joinLink = new JoinLinkImpl();
    }

    public void addSource(Node<?, I> node)
    {
        forkLink.addProducer(node);
    }

    public void addFork(Node<I, Pair<I, O>> node)
    {
        forkLink.addConsumer(node);
        joinLink.addProducer(node);
    }

    public void addConsumer(Node<NodeJoinResult<I, O>, ?> node)
    {
        joinLink.addConsumer(node);
    }

    protected class ForkLinkImpl extends AbstractLink<I, I>
    {
        @Override
        protected boolean forward(Node<?, ? extends I> collectionNode, I i)
        {
            sendSourceMessages(i);
            send(i);
            return true;
        }
    }

    protected class JoinLinkImpl extends AbstractLink<Pair<I, O>, NodeJoinResult<I, O>>
    {
        @Override
        protected boolean forward(Node<?, ? extends Pair<I, O>> pairNode, Pair<I, O> t)
        {
            final NodeJoinResult<I, O> forkJoinResult = joinForkResponse(pairNode, t);
            if (forkJoinResult != null)
            {
                send(forkJoinResult);
                return true;
            }

            return false;
        }

        public Collection<String> getForkNodeNames()
        {
            return Collections.unmodifiableCollection(producerNodes.keySet());
        }

        public Collection<Node<?, Pair<I, O>>> getForkNodes()
        {
            return Collections.unmodifiableCollection(producerNodes.values());
        }
    }

    private void sendSourceMessages(I i)
    {
        forkMap.put(i.getGroupId(), Pair.of(i, new ForkUnit<String, O>(joinLink.getForkNodeNames(), joinPredicate)));
    }

    private NodeJoinResult<I, O> joinForkResponse(Node<?, ? extends Pair<I, O>> node, Pair<I, O> t)
    {
        final Pair<I, ForkUnit<String, O>> forkUnitPair = forkMap.get(t.first.getGroupId());

        if (forkUnitPair != null)
        {
            final ForkUnit<String, O> forkUnit = forkUnitPair.second;
            final Pair<String, O> result = Pair.of(node.getName(), t.second);
            if (forkUnit.join(result))
            {
                forkMap.remove(t.first.getGroupId());
                return new NodeJoinResult<I, O>(forkUnit.getResult(), t.first, forkUnit.getFinishedForks(),
                    forkUnit.getUnfinishedForks());
            }
        }

        return null;
    }
}
