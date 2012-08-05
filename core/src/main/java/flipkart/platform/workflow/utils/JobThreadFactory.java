package flipkart.platform.workflow.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
* User: shashwat
* Date: 03/08/12
*/
public class JobThreadFactory implements ThreadFactory
{
    private static class JobThreadGroup extends ThreadGroup
    {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public JobThreadGroup(String name)
        {
            super(name);
        }

        public int getNextAvailableThreadInc()
        {
            return threadNumber.incrementAndGet();
        }
    }

    private final JobThreadGroup threadGroup;

    private final String name;

    public JobThreadFactory(String name)
    {
        this.name = name;
        this.threadGroup = new JobThreadGroup(name);
    }

    public Thread newThread(Runnable r)
    {
        return new Thread(threadGroup, r, name + "-" + threadGroup.getNextAvailableThreadInc())
        {
        };
    }
}
