package flipkart.platform.hydra.node.builder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.hydra.utils.HydraThreadFactory;

/**
 * User: shashwat
 * Date: 02/09/12
 */
public abstract class AbstractWorkStationBuilder<I, O> extends AbstractNodeBuilder<I, O>
{
    protected ExecutorService executorService = MoreExecutors.sameThreadExecutor();

    protected AbstractWorkStationBuilder(String name)
    {
        super(name);
    }

    @Override
    public AbstractWorkStationBuilder<I, O> withExecutor(ExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    @Override
    public AbstractWorkStationBuilder<I, O> withThreadExecutor(int numThreads)
    {
        return withExecutor(Executors.newFixedThreadPool(numThreads, new HydraThreadFactory(name)));
    }

    @Override
    public AbstractWorkStationBuilder<I, O> withThreadExecutor(int numThreads, ThreadFactory threadFactory)
    {
        return withExecutor(Executors.newFixedThreadPool(numThreads, threadFactory));
    }


}
