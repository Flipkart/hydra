package flipkart.platform.hydra.utils;

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
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
    private final T ob;

    public ThreadLocalWeakReference(T ob, FinalizableReferenceQueue queue)
    {
        super(Thread.currentThread(), queue);
        this.ob = ob;
    }

    @Override
    public void finalizeReferent()
    {
        Initializable.LifeCycle.destroy(ob);
    }

    public T getObject()
    {
        return ob;
    }
}
