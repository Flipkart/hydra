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

public class DefaultJobFactory<J extends Initializable> implements
        JobFactory<J>
{
    private final Constructor<? extends J> jobConstructor;

    public DefaultJobFactory(Class<? extends J> jobClass)
            throws NoSuchMethodException
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
            // TODO: log
            e.printStackTrace();
        }

        return null;
    }

    public static <J extends Initializable> DefaultJobFactory<J> create(
            Class<? extends J> jobClass) throws NoSuchMethodException
    {
        return new DefaultJobFactory<J>(jobClass);
    }
}