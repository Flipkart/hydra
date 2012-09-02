package flipkart.platform.hydra.node.builder;

import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.jobs.OneToManyJob;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.utils.DefaultJobFactory;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public class WSBuilder
{
    protected ExecutorService executorService = MoreExecutors.sameThreadExecutor();

    private WSBuilder()
    {
    }

    public static <I, O, J extends OneToOneJob<I, O>> O2ONodeBuilder<I, O> withO2OJob(Class<J> jobClass) throws
        NoSuchMethodException
    {
        return withO2OJobFactory(jobClass.getSimpleName(), DefaultJobFactory.create(jobClass));
    }

    public static <I, O, J extends OneToOneJob<I, O>> O2ONodeBuilder<I, O> withO2OJobFactory(String name,
        JobFactory<J> jobFactory) throws NoSuchMethodException
    {
        return new O2ONodeBuilder<I, O>(name, jobFactory);
    }

    public static <I, O, J extends OneToManyJob<I, O>> O2MNodeBuilder<I, O> withO2MJob(Class<J> jobClass) throws
        NoSuchMethodException
    {
        return withO2MJobFactory(jobClass.getSimpleName(), DefaultJobFactory.create(jobClass));
    }

    public static <I, O, J extends OneToManyJob<I, O>> O2MNodeBuilder<I, O> withO2MJobFactory(String name,
        JobFactory<J> jobFactory) throws NoSuchMethodException
    {
        return new O2MNodeBuilder<I, O>(name, jobFactory);
    }

    public static <I, O, J extends ManyToManyJob<I, O>> M2MNodeBuilder<I, O> withM2MJob(Class<J> jobClass) throws
        NoSuchMethodException
    {
        return withM2MJobFactory(jobClass.getSimpleName(), DefaultJobFactory.create(jobClass));
    }

    public static <I, O, J extends ManyToManyJob<I, O>> M2MNodeBuilder<I, O> withM2MJobFactory(String name,
        JobFactory<J> jobFactory) throws NoSuchMethodException
    {
        return new M2MNodeBuilder<I, O>(name, jobFactory);
    }
}
