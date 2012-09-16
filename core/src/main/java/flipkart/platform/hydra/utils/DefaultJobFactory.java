package flipkart.platform.hydra.utils;

import java.lang.reflect.Constructor;
import flipkart.platform.hydra.job.JobFactory;

/**
 * {@link flipkart.platform.hydra.job.JobFactory} that uses reflection to create {@link flipkart.platform.workflow
  * .job.Initializable} jobs
 * using their class
 * 
 * @author shashwat
 * 
 * @param <J>
 *            Classes that extend {@link flipkart.platform.hydra.traits.Initializable}
 */

public class DefaultJobFactory<J> implements JobFactory<J>
{
    private final Constructor<J> jobConstructor;

    public DefaultJobFactory(Class<J> jobClass) throws NoSuchMethodException
    {
        this.jobConstructor = jobClass.getConstructor();
    }

    public J newJob()
    {
        try
        {
            return jobConstructor.newInstance();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <J> DefaultJobFactory<J> create(Class<J> jobClass) throws NoSuchMethodException
    {
        return new DefaultJobFactory<J>(jobClass);
    }
}
