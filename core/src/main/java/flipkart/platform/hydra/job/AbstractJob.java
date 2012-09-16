package flipkart.platform.hydra.job;

/**
 * User: shashwat
 * Date: 09/08/12
 */
public abstract class AbstractJob<I> implements Job<I>
{
    @Override
    public void failed(I i, Throwable cause)
    {
    }
}
