package flipkart.platform.hydra.utils;

import java.util.concurrent.atomic.AtomicBoolean;

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
    private volatile I i = null;
    private final I defValue;

    public Once()
    {
        this.defValue = null;
    }

    public Once(I defValue)
    {
        this.defValue = defValue;
    }

    public Once(I initial, I defValue)
    {
        this.i = initial;
        this.defValue = defValue;
    }

    /**
     * Set the value of the variable to the configured default value as set in {@link #Once(Object)}
     *
     * @return <code>true</code> only if the value is set, <code>false</code> otherwise
     * @see #Once(Object)
     */
    public boolean set()
    {
        return set(defValue);
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
            i = value;
            return true;
        }

        return false;
    }

    /**
     * @return the value that was set or <code>null</code> otherwise. Note that <code>null</code> is also set if the
     *         default constructor was used and {@link #set()} was used.
     */
    public I get()
    {
        return i;
    }

    public boolean isSet()
    {
        return isSet.get();
    }

}
