package flipkart.platform.hydra.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import com.google.common.collect.Iterators;

/**
 * User: shashwat
 * Date: 08/08/12
 */
public class UnModifiableSet<I> implements Iterable<I>
{
    private final Set<I> backingSet;

    public UnModifiableSet(Set<I> backingSet)
    {
        this.backingSet = backingSet;
    }

    public static <I> UnModifiableSet<I> from(Set<I> backingSet)
    {
        return new UnModifiableSet<I>(backingSet);
    }

    public static <I> UnModifiableSet<I> from(Collection<I> backingSet)
    {
        return new UnModifiableSet<I>(new HashSet<I>(backingSet));
    }

    public static <I> UnModifiableSet<I> copyOf(Set<I> backingSet)
    {
        return new UnModifiableSet<I>(new HashSet<I>(backingSet));
    }

    public int size()
    {
        return backingSet.size();
    }


    public boolean isEmpty()
    {
        return backingSet.isEmpty();
    }


    public boolean contains(I o)
    {
        return backingSet.contains(o);
    }


    public Iterator<I> iterator()
    {
        return Iterators.unmodifiableIterator(backingSet.iterator());
    }


    public boolean containsAll(Collection<?> c)
    {
        return backingSet.containsAll(c);
    }

    @Override
    public String toString()
    {
        return backingSet.toString();
    }
}
