package flipkart.platform.hydra.topology;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.NodeSupervisor;
import flipkart.platform.hydra.supervisor.Supervisor;
import flipkart.platform.hydra.supervisor.SupervisorFactory;
import flipkart.platform.hydra.traits.HasIdentity;
import flipkart.platform.hydra.utils.RunState;

/**
 * User: shashwat
 * Date: 17/08/12
 */
public class SupervisorTopology implements Topology
{
    private final ConcurrentMap<String, Supervisor> supervisorMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, String> supervisorLookupMap = Maps.newConcurrentMap();

    private final RunState runState = new RunState();

    @Override
    public <I> void connect(Node<?, I> from, Node<I, ?> to)
    {
        if (runState.isActive())
        {
            getSupervisor(from).supervise(getSupervisor(to));
        }
    }

    @Override
    public void shutdown(boolean awaitTermination)
    {
        if (runState.shuttingDown())
        {
            final Collection<Supervisor> topologicalSortedList = topologicalSort(supervisorMap.values(), true);
            for (Supervisor supervisor : topologicalSortedList)
            {
                if (supervisor.tryShutdown(awaitTermination))
                {
                    final String name = supervisorLookupMap.remove(supervisor.getName());
                    supervisorMap.remove(name);
                }
            }
            runState.shutdown();
        }
    }

    @Override
    public boolean isShutdown()
    {
        return runState.isShutdown();
    }

    private Supervisor getSupervisor(Node node)
    {
        return getSupervisor(node, NodeSupervisor.factory);
    }

    private <T extends HasIdentity> Supervisor getSupervisor(T t, SupervisorFactory<T> factory)
    {
        final String name = t.getName();
        final Supervisor supervisor = supervisorMap.get(name);

        if (supervisor != null)
        {
            return supervisor;
        }

        synchronized (this)
        {
            Supervisor newSupervisor = supervisorMap.get(name);
            if (newSupervisor == null)
            {
                newSupervisor = factory.newSupervisor(t);
                supervisorMap.put(name, newSupervisor);
                supervisorLookupMap.put(newSupervisor.getName(), name);
            }
            return newSupervisor;
        }
    }

    private static Map<Supervisor, Set<Supervisor>> prepareSuccessorMap(Collection<Supervisor> graph)
    {
        final HashMap<Supervisor, Set<Supervisor>> successors = new HashMap();

        // build successors map
        for (Supervisor supervisor : graph)
        {
            successors.put(supervisor, Sets.newHashSet(supervisor.getSuccessors()));
        }

        boolean change;
        do
        {
            change = false;
            for (Supervisor n : graph)
            {
                final Set<Supervisor> nSuccessors = successors.get(n);
                for (Supervisor ns : nSuccessors.toArray(new Supervisor[0]))
                {
                    change = nSuccessors.addAll(successors.get(ns)) || change;
                }
            }
        } while (change);

        return successors;
    }

    private static Collection<Supervisor> topologicalSort(Collection<Supervisor> graph, boolean breakLoops)
    {
        final List<Supervisor> sorted = new ArrayList(); // result

        final Map<Supervisor, Set<Supervisor>> successors = prepareSuccessorMap(graph);
        final Set<Supervisor> visited = new HashSet(); // auxiliary list

        for (Supervisor n : graph)
        {
            visitLoopsPermitted(n, visited, successors, sorted, breakLoops);
        }

        return sorted;
    }

    private static void visitLoopsPermitted(Supervisor supervisor, Set<Supervisor> visited,
        Map<Supervisor, Set<Supervisor>> successors, List<Supervisor> sorted, boolean breakLoops)
    {
        if (visited.contains(supervisor))
            return;
        visited.add(supervisor);
        for (Supervisor predecessor : supervisor.getPredecessors())
        {
            if (successors.get(supervisor).contains(predecessor))
            {
                if(breakLoops)
                {
                    predecessor.unsupervise(supervisor);
                }
                continue;
            }
            visitLoopsPermitted(predecessor, visited, successors, sorted, breakLoops);
        }
        sorted.add(supervisor);
    }

}
