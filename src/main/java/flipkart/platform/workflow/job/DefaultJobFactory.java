package flipkart.platform.workflow.job;

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
    private final Class<? extends J> jobClass;

    public DefaultJobFactory(Class<? extends J> jobClass)
    {
        this.jobClass = jobClass;
    }

    public J newJob()
    {
        try
        {
            return jobClass.newInstance();
        }
        catch (Exception e)
        {
            // TODO: log
            e.printStackTrace();
        }

        return null;
    }

    public static <J extends Initializable> DefaultJobFactory<J> create(
            Class<? extends J> jobClass)
    {
        return new DefaultJobFactory<J>(jobClass);
    }
}