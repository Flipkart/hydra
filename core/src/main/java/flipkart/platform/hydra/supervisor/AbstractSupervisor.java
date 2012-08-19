package flipkart.platform.hydra.supervisor;

import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.utils.UnModifiableCollection;
import flipkart.platform.hydra.utils.UnModifiableSet;

/**
 * User: shashwat
 * Date: 15/08/12
 */
public abstract class AbstractSupervisor<T> implements Supervisor
{
    private final ConcurrentMap<String, Supervisor> parentSupervisors = Maps.newConcurrentMap();
    private final ConcurrentMap<String, Supervisor> childSupervisors = Maps.newConcurrentMap();

    private final T t;
    private volatile boolean shutdown = false;

    public AbstractSupervisor(T t)
    {
        this.t = t;
    }

    @Override
    public void supervise(Supervisor child)
    {
        if (childSupervisors.putIfAbsent(child.getName(), child) == null)
        {
            child.addParent(this);
        }
    }

    @Override
    public void addParent(Supervisor parent)
    {
        parentSupervisors.putIfAbsent(parent.getName(), parent);
    }

    @Override
    public boolean isShutdown()
    {
        return shutdown;
    }

    @Override
    public synchronized boolean tryShutdown(boolean awaitTermination)
    {
        shutdown = (canShutdown() && shutdownResource(t, awaitTermination));
        if(shutdown)
        {
            for (Supervisor supervisor : childSupervisors.values())
            {
                unsupervise(supervisor);
            }
        }
        return shutdown;
    }

    @Override
    public void unsupervise(Supervisor child)
    {
        childSupervisors.remove(child.getName());
        child.removeParent(this);
    }

    @Override
    public void removeParent(Supervisor parent)
    {
        parentSupervisors.remove(parent.getName());
    }

    @Override
    public UnModifiableCollection<Supervisor> getPredecessors()
    {
        return UnModifiableCollection.from(parentSupervisors.values());
    }

    @Override
    public UnModifiableCollection<Supervisor> getSuccessors()
    {
        return UnModifiableCollection.from(childSupervisors.values());
    }

    @Override
    public boolean isIndependent()
    {
        return parentSupervisors.isEmpty();
    }

    private boolean canShutdown()
    {
        for (Supervisor supervisor : parentSupervisors.values())
        {
            if (!supervisor.isShutdown())
            {
                return false;
            }
        }
        return !isShutdown();
    }

    protected abstract boolean shutdownResource(T t, boolean awaitTermination);
}
