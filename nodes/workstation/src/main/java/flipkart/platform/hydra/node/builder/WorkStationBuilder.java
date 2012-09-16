package flipkart.platform.hydra.node.builder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.hydra.utils.DefaultThreadFactory;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public abstract class WorkStationBuilder<I, O> extends NodeBuilder<I, O>
{
    protected ExecutorService executorService = MoreExecutors.sameThreadExecutor();

    protected WorkStationBuilder(String name)
    {
        super(name);
    }

    public WorkStationBuilder<I, O> withExecutor(ExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    public WorkStationBuilder<I, O> withThreadExecutor(int numThreads)
    {
        return withExecutor(Executors.newFixedThreadPool(numThreads, new DefaultThreadFactory(name)));
    }

    public WorkStationBuilder<I, O> withThreadExecutor(int numThreads, ThreadFactory threadFactory)
    {
        return withExecutor(Executors.newFixedThreadPool(numThreads, threadFactory));
    }
}
