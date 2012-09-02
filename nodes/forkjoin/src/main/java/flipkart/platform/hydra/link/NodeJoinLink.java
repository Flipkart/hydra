package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.topology.LinkTopology;
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

    private final ForkJoinMetrics metrics = new ForkJoinMetrics(ForkJoinLink.class);

    public NodeJoinLink(LinkTopology topology, Predicate<ForkUnit<String, O>> joinPredicate)
    {
        this.joinPredicate = joinPredicate;

        this.forkLink = new ForkLinkImpl(topology);
        this.joinLink = new JoinLinkImpl(topology);
    }

    public void addProducer(Node<?, I> node)
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
        public ForkLinkImpl(LinkTopology topology)
        {
            super(topology);
        }

        @Override
        protected boolean forward(Node<?, ? extends I> collectionNode, I i)
        {
            createForkUnit(i);
            send(i);
            return true;
        }
    }

    protected class JoinLinkImpl extends AbstractLink<Pair<I, O>, NodeJoinResult<I, O>>
    {
        public JoinLinkImpl(LinkTopology topology)
        {
            super(topology);
        }

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
    }

    private void createForkUnit(I i)
    {
        final Collection<String> nodeNames = joinLink.getForkNodeNames();
        forkMap.put(i.getGroupId(), Pair.of(i, new ForkUnit<String, O>(nodeNames, joinPredicate)));
        metrics.reportForks(nodeNames.size());
    }

    private NodeJoinResult<I, O> joinForkResponse(Node<?, ? extends Pair<I, O>> node, Pair<I, O> t)
    {
        final Pair<I, ForkUnit<String, O>> forkUnitPair = forkMap.get(t.first.getGroupId());

        if (forkUnitPair != null)
        {
            final ForkUnit<String, O> forkUnit = forkUnitPair.second;
            final Pair<String, O> result = Pair.of(node.getIdentity(), t.second);
            if (forkUnit.join(result))
            {
                forkMap.remove(t.first.getGroupId());
                metrics.reportJoin(System.currentTimeMillis() - forkUnit.getCreatedTimestamp());
                return new NodeJoinResult<I, O>(forkUnit.getResult(), t.first, forkUnit.getFinishedForks(),
                    forkUnit.getUnfinishedForks());
            }
        }

        return null;
    }
}
