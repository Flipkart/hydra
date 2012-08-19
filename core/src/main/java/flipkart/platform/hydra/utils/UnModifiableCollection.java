package flipkart.platform.hydra.utils;

import java.util.Collection;
import java.util.Iterator;
import com.google.common.collect.Iterators;
import flipkart.platform.hydra.supervisor.Supervisor;

/**
 * User: shashwat
 * Date: 19/08/12
 */
public class UnModifiableCollection<I> implements Iterable<I>
{
    private final Collection<I> collection;

    public UnModifiableCollection(Collection<I> collection)
    {
        this.collection = collection;
    }

    public int size()
    {
        return collection.size();
    }


    public boolean isEmpty()
    {
        return collection.isEmpty();
    }


    public boolean contains(I o)
    {
        return collection.contains(o);
    }


    public Iterator<I> iterator()
    {
        return Iterators.unmodifiableIterator(collection.iterator());
    }


    public boolean containsAll(Collection<?> c)
    {
        return collection.containsAll(c);
    }

    public static <I> UnModifiableCollection<I> from(Collection<I> values)
    {
        return new UnModifiableCollection<I>(values);
    }
}
