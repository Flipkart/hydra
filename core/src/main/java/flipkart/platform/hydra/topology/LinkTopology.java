package flipkart.platform.hydra.topology;

import java.util.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import flipkart.platform.hydra.link.GenericLink;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.traits.CanShutdown;
import flipkart.platform.hydra.utils.RunState;
import flipkart.platform.hydra.utils.UnModifiableCollection;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public class LinkTopology implements CanShutdown
{
    private final Set<GenericLink> linkSet = Sets.newSetFromMap(Maps.<GenericLink, Boolean>newConcurrentMap());
    private final RunState runState = new RunState();

    public LinkTopology addLink(GenericLink link)
    {
        if (runState.isActive())
        {
            linkSet.add(link);
        }
        return this;
    }

    @Override
    public boolean isShutdown()
    {
        return runState.isShutdown();
    }

    @Override
    public boolean shutdown(boolean awaitTermination)
    {
        if (runState.shuttingDown())
        {
            final Collection<Node> nodes = topologicalSort(linkSet);
            for (Node node : nodes)
            {
                shutdownNode(node, awaitTermination);
            }
            runState.shutdown();
        }
        return true;
    }

    private static void shutdownNode(Node node, boolean awaitTermination)
    {
        try
        {
            node.shutdown(awaitTermination);
        }
        catch (InterruptedException e)
        {
            // do nothing
        }
    }

    private static Collection<Node> topologicalSort(Collection<GenericLink> genericLinks)
    {
        final List<Node> sorted = new ArrayList(); // result

        final Map<Node, Set<Node>> successors = prepareSuccessorMap(genericLinks);

        final Map<Node, Set<Node>> predecessorMap = preparePredecessorMap(genericLinks);

        final Set<Node> visited = new HashSet(); // auxiliary list

        for (Node n : successors.keySet())
        {
            visitLoopsPermitted(n, predecessorMap, visited, successors, sorted);
        }

        return sorted;
    }

    private static Map<Node, Set<Node>> prepareSuccessorMap(Collection<GenericLink> links)
    {
        final HashMap<Node, Set<Node>> successorMap = new HashMap();

        // build successorMap map
        for (GenericLink link : links)
        {
            final UnModifiableCollection<Node> producers = link.getProducers();
            final UnModifiableCollection<Node> consumers = link.getConsumers();

            for (Node consumer : consumers)
            {
                if (!successorMap.containsKey(consumer))
                {
                    successorMap.put(consumer, Sets.<Node>newHashSet());
                }
            }

            for (Node producer : producers)
            {
                if (!successorMap.containsKey(producer))
                {
                    successorMap.put(producer, Sets.<Node>newHashSet());
                }

                final Set<Node> successors = successorMap.get(producer);
                for (Node consumer : consumers)
                {
                    successors.add(consumer);
                }
            }
        }

        boolean change;
        do
        {
            change = false;
            for (Node n : successorMap.keySet())
            {
                final Set<Node> nSuccessors = successorMap.get(n);
                for (Node ns : nSuccessors.toArray(new Node[nSuccessors.size()]))
                {
                    change = nSuccessors.addAll(successorMap.get(ns)) || change;
                }
            }
        } while (change);

        return successorMap;
    }

    private static Map<Node, Set<Node>> preparePredecessorMap(Collection<GenericLink> links)
    {
        final HashMap<Node, Set<Node>> predecessorMap = new HashMap();

        // build successorMap map
        for (GenericLink link : links)
        {
            final UnModifiableCollection<Node> consumers = link.getConsumers();
            final UnModifiableCollection<Node> producers = link.getProducers();

            for (Node producer : producers)
            {
                // Add producers as well to predecessor map as it is the set of all nodes
                if (!predecessorMap.containsKey(producer))
                {
                    predecessorMap.put(producer, Sets.<Node>newHashSet());
                }
            }
            
            for (Node consumer : consumers)
            {
                if (!predecessorMap.containsKey(consumer))
                {
                    predecessorMap.put(consumer, Sets.<Node>newHashSet());
                }

                final Set<Node> predecessorSet = predecessorMap.get(consumer);

                for (Node producer : producers)
                {
                    predecessorSet.add(producer);
                }
            }
        }

        return predecessorMap;
    }

    private static void visitLoopsPermitted(Node node, Map<Node, Set<Node>> predecessorMap, Set<Node> visited,
        Map<Node, Set<Node>> successors,
        List<Node> sorted)
    {
        if (visited.contains(node))
            return;
        visited.add(node);
        for (Node predecessor : predecessorMap.get(node))
        {
            if (successors.get(node).contains(predecessor))
            {
                continue;
            }
            visitLoopsPermitted(predecessor, predecessorMap, visited, successors, sorted);
        }
        sorted.add(node);
    }

}
