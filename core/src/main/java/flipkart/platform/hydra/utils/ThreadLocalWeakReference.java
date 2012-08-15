package flipkart.platform.hydra.utils;

import java.lang.ref.Reference;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
import com.google.common.collect.Sets;
import flipkart.platform.hydra.traits.Initializable;

/**
 * User: shashwat
 * Date: 02/08/12
 */

/**
 * Wraps the current thread as WeakReference and keeps the object <code>instanceof</code> {@link Initializable} that
 * needs to be destroyed
 *
 * @param <T>
 */
public final class ThreadLocalWeakReference<T> extends FinalizableWeakReference<Thread>
{
    private static final Set<Reference> finalizableSet =
        Sets.newSetFromMap(new ConcurrentHashMap<Reference, Boolean>());

    private static final FinalizableReferenceQueue referenceQueue = new FinalizableReferenceQueue();

    static
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                // we need a copy here to avoid concurrent modification exception
                final Reference[] references = finalizableSet.toArray(new Reference[finalizableSet.size()]);
                for (Reference reference : references)
                {
                    reference.enqueue();
                }
            }
        });
    }

    private final T ob;

    public ThreadLocalWeakReference(T ob)
    {
        super(Thread.currentThread(), referenceQueue);
        finalizableSet.add(this);

        this.ob = ob;
    }

    @Override
    public void finalizeReferent()
    {
        finalizableSet.remove(this);
        Initializable.LifeCycle.destroy(ob);
    }

    public T getObject()
    {
        return ob;
    }
}
