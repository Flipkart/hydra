package flipkart.platform.workflow.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * User: shashwat
 * Date: 24/02/12
 */
public class RefCounter
{
    private final AtomicLong counter;

    public RefCounter(int initial)
    {
        counter = new AtomicLong(initial);
    }

    public long peek()
    {
        return counter.get();
    }

    // a fuzzy kind of take, can fail to take in case of concurrent access. No false positives but many false negatives
    public boolean bloomTake()
    {
        final long current = counter.get();
        return (current > 0 && counter.compareAndSet(current, current - 1));
    }

    public boolean take()
    {
        long current;
        do
        {
            current = counter.get();
        }
        while (current > 0 && !counter.compareAndSet(current, current - 1));
        return (current > 0);
    }

    public long take(long n)
    {
        long current;
        long count;
        do
        {
            current = counter.get();
            count = Math.min(current, n);
        }
        while (current > 0 && !counter.compareAndSet(current, current - count));

        return count;
    }

    public long offer()
    {
        return counter.incrementAndGet();
    }

    public long offer(int n)
    {
        return counter.addAndGet(n);
    }

    public boolean isZero()
    {
        return peek() == 0;
    }
}
