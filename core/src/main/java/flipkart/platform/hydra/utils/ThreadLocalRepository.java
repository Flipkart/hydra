package flipkart.platform.hydra.utils;

import java.lang.ref.Reference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.Sets;

import static flipkart.platform.hydra.traits.Initializable.LifeCycle.destroy;
import static flipkart.platform.hydra.traits.Initializable.LifeCycle.initialize;

/**
 * User: shashwat
 * Date: 02/08/12
 */

/**
 * A thread local implementation that auto calls <code>destroy()</code> if the thread goes out of scope.
 *
 * @param <T>
 *     Object type
 */
public class ThreadLocalRepository<T>
{
    private final Set<Reference> finalizableSet = Sets.newSetFromMap(new ConcurrentHashMap<Reference, Boolean>());

    private final ObjectFactory<? extends T> factory;
    private final ThreadLocal<ThreadLocalWeakReference<T>> threadLocal;

    public static <J> ThreadLocalRepository<J> from(ObjectFactory<? extends J> factory)
    {
        return new ThreadLocalRepository<J>(factory);
    }

    public ThreadLocalRepository(ObjectFactory<? extends T> factory)
    {
        this.factory = factory;
        initialize(factory);

        this.threadLocal = new ThreadLocal<ThreadLocalWeakReference<T>>()
        {
            @Override
            protected ThreadLocalWeakReference<T> initialValue()
            {
                final T j = ThreadLocalRepository.this.factory.newObject();
                if (j != null)
                {
                    try
                    {
                        initialize(j);
                        return new ThreadLocalWeakReference<T>(j, finalizableSet);
                    }
                    catch (Exception e)
                    {
                        //throw new RuntimeException("Exception while initializing job: " + j.getClass().getName(), e);
                        // TODO: log
                    }
                }
                return null;
            }
        };
    }

    public T get()
    {
        final ThreadLocalWeakReference<T> reference = threadLocal.get();
        if (reference != null && reference.get() != null)
        {
            return reference.getObject();
        }

        return null;
    }

    void remove()
    {
        threadLocal.remove();
    }

    public void close()
    {
        destroy(factory);
        finalizeAll();
    }

    private void finalizeAll()
    {
        // we need a copy here to avoid concurrent modification exception
        final Reference[] references = finalizableSet.toArray(new Reference[finalizableSet.size()]);
        for (Reference reference : references)
        {
            reference.enqueue();
        }
    }

}
