package flipkart.platform.hydra.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
* User: shashwat
* Date: 03/08/12
*/
public class DefaultThreadFactory implements ThreadFactory
{
    private static class HydraThreadGroup extends ThreadGroup
    {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public HydraThreadGroup(String name)
        {
            super(name);
        }

        public String getNextAvailableThreadName()
        {
            return getName() + "-" + threadNumber.incrementAndGet();
        }
    }

    private final HydraThreadGroup threadGroup;

    public DefaultThreadFactory(String name)
    {
        this.threadGroup = new HydraThreadGroup(name);
    }

    public Thread newThread(Runnable r)
    {
        return new Thread(threadGroup, r, threadGroup.getNextAvailableThreadName())
        {
        };
    }

    public ThreadGroup getThreadGroup()
    {
        return threadGroup;
    }
}
