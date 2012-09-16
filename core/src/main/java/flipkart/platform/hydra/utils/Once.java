package flipkart.platform.hydra.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * User: shashwat
 * Date: 19/08/12
 */

/**
 * Helper class that sets value of a variable only and only once.
 *
 * @param <I>
 */
public class Once<I>
{
    private final AtomicBoolean isSet = new AtomicBoolean(false);
    private final AtomicReference<I> i;

    public Once()
    {
        i = new AtomicReference<I>();
    }

    public Once(I initialValue)
    {
        this.i = new AtomicReference<I>(initialValue);
    }

    /**
     * Set the value of the variable to the given value.
     *
     * @param value
     *     the value to be set
     * @return <code>true</code> only if the value was set, <code>false</code> otherwise
     */
    public boolean set(I value)
    {
        if (isSet.compareAndSet(false, true))
        {
            i.set(value);
            return true;
        }

        return false;
    }

    /**
     * @return the value that was set or initial value if provided {@link #} ot <code>null</code> otherwise.
     * Note that <code>null</code> is also set if the default constructor was used and {@link #set(Object)} was used.
     */
    public I get()
    {
        return i.get();
    }

    public boolean isSet()
    {
        return isSet.get();
    }

}
