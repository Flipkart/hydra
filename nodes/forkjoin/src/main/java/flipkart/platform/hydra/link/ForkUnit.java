package flipkart.platform.hydra.link;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import flipkart.platform.hydra.utils.Pair;
import flipkart.platform.hydra.utils.UnModifiableSet;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
 * User: shashwat
 * Date: 08/08/12
 */
public class ForkUnit<I, S>
{
    private final Set<I> unfinishedForks;
    private final ConcurrentMap<I, S> finishedForks = new ConcurrentHashMap<I, S>();
    private final JoinPredicate<I, S> joinPredicate;

    private volatile boolean isDone = false;

    public ForkUnit(Collection<I> unfinishedForks, @Nullable JoinPredicate<I, S> joinPredicate)
    {
        this.joinPredicate = joinPredicate;
        this.unfinishedForks = new HashSet<I>(unfinishedForks);
    }

    public synchronized boolean join(Pair<I, S> forkResult)
    {
        if (!isDone && (finishedForks.putIfAbsent(forkResult.first, forkResult.second) == null))
        {
            unfinishedForks.remove(forkResult.first);
            isDone = unfinishedForks.isEmpty() || (joinPredicate != null && joinPredicate.apply(this));
            return true;
        }

        return false;
    }

    public boolean isDone()
    {
        return isDone;
    }

    public UnModifiableMap<I, S> getFinishedForks()
    {
        return UnModifiableMap.copyOf(finishedForks);
    }

    public UnModifiableSet<I> getUnfinishedForks()
    {
        return UnModifiableSet.from(unfinishedForks);
    }
}
