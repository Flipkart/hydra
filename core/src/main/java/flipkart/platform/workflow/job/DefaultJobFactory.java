package flipkart.platform.workflow.job;

import java.lang.reflect.Constructor;

/**
 * {@link JobFactory} that uses reflection to create {@link Initializable} jobs
 * using their class
 * 
 * @author shashwat
 * 
 * @param <J>
 *            Classes that extend {@link Initializable}
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
