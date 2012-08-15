package flipkart.platform.hydra.utils;

/**
 * User: shashwat
 * Date: 12/08/12
 */
public class SingleObjectFactory<T> implements ObjectFactory<T>
{
    private final T t;

    public static <T> SingleObjectFactory<T> from(T t)
    {
        return new SingleObjectFactory<T>(t);
    }
    
    public SingleObjectFactory(T t)
    {
        this.t = t;
    }

    @Override
    public T newObject()
    {
        return t;
    }
}
