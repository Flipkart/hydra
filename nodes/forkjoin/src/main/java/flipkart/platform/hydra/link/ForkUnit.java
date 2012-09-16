package flipkart.platform.hydra.link;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.google.common.base.Predicate;
import flipkart.platform.hydra.utils.Pair;
import flipkart.platform.hydra.utils.UnModifiableMap;
import flipkart.platform.hydra.utils.UnModifiableSet;

/**
 * User: shashwat
 * Date: 08/08/12
 */
public class ForkUnit<I, O>
{
    private final Set<I> unfinishedForks;
    private final ConcurrentMap<I, O> finishedForks = new ConcurrentHashMap<I, O>();
    private final Predicate<ForkUnit<I, O>> joinPredicate;

    private final long createdTimestamp = System.currentTimeMillis();

    private volatile boolean isDone = false;
    private volatile boolean result = false;

    public ForkUnit(Collection<I> unfinishedForks)
    {
        this(unfinishedForks, null);
    }

    public ForkUnit(Collection<I> unfinishedForks, Predicate<ForkUnit<I, O>> joinPredicate)
    {
        this.joinPredicate = joinPredicate;
        this.unfinishedForks = new HashSet<I>(unfinishedForks);
    }

    /**
     * Merges and marks a fork as done. Returns true if {@code JoinPredicate#apply} returns true or all the jobs are
     * done.
     *
     * @param forkResult
     *     the result to merge with all other results
     * @return <code>true</code> is returned <string>once</string> if and only if one of the following is true:
     *         <ul>
     *         <li>{@link JoinPredicate#apply} is <code>null</code> and there are no more forks in
     *         #unfinishedForks</li>. In this case, <code>isDone = result = true</code>
     *         <li>{@link JoinPredicate#apply} is set and it returns true. No more forks will be merged and henceforth
     *         this method will always return <code>false</code></li>
     *         <li>{@link JoinPredicate#apply} is set and it returns false but there are no more jobs to execute</li>
     *         </ul>
     *         <p>
     *         <code>false</code> is returned iff {@code JoinPredicate#apply} has returned true before all forks are
     *         finished
     *         </p>
     */
    public synchronized boolean join(Pair<I, O> forkResult)
    {
        if (!isDone && (finishedForks.putIfAbsent(forkResult.first, forkResult.second) == null))
        {
            unfinishedForks.remove(forkResult.first);
            isDone = unfinishedForks.isEmpty();

            if (joinPredicate == null)
            {
                result = isDone;
            }
            else
            {
                result = joinPredicate.apply(this);
                isDone = isDone || result;
            }
            return isDone;
        }

        return false;
    }

    public boolean isDone()
    {
        return isDone;
    }

    public boolean getResult()
    {
        return result;
    }

    public boolean isFinished()
    {
        return unfinishedForks.isEmpty();
    }

    public long getCreatedTimestamp()
    {
        return createdTimestamp;
    }

    public UnModifiableMap<I, O> getFinishedForks()
    {
        return UnModifiableMap.from(finishedForks);
    }

    public UnModifiableSet<I> getUnfinishedForks()
    {
        return UnModifiableSet.from(unfinishedForks);
    }

    @Override
    public String toString()
    {
        return "ForkUnit{" +
            "unfinishedForks=" + unfinishedForks +
            ", finishedForks=" + finishedForks +
            ", createdTimestamp=" + createdTimestamp +
            ", isDone=" + isDone +
            '}';
    }
}
