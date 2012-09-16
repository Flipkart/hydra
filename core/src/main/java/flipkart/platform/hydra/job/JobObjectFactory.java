package flipkart.platform.hydra.job;

import flipkart.platform.hydra.utils.ObjectFactory;

/**
 * An adapter that converts {@link JobFactory} instance into {@link ObjectFactory} instance
 * User: shashwat
 * Date: 02/08/12
 */
public class JobObjectFactory<J> implements ObjectFactory<J>
{
    private final JobFactory<J> jobFactory;

    public static <J> JobObjectFactory<J> from(JobFactory<J> jobFactory)
    {
        return new JobObjectFactory<J>(jobFactory);
    }

    public JobObjectFactory(JobFactory<J> jobFactory)
    {
        this.jobFactory = jobFactory;
    }

    @Override
    public J newObject()
    {
        return jobFactory.newJob();
    }
}
