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
    private static final FinalizableReferenceQueue referenceQueue = new FinalizableReferenceQueue();

    private final T ob;
    private final Set<Reference> referenceSet;

    public ThreadLocalWeakReference(T ob, Set<Reference> referenceSet)
    {
        super(Thread.currentThread(), referenceQueue);
        this.referenceSet = referenceSet;
        referenceSet.add(this);

        this.ob = ob;
    }

    @Override
    public void finalizeReferent()
    {
        referenceSet.remove(this);
        Initializable.LifeCycle.destroy(ob);
    }

    public T getObject()
    {
        return ob;
    }
}
