package flipkart.platform.hydra.traits;

/**
 * Interface for initialize-able classes. The object will be initialized using
 * {@link #init()} before being used. {@link #destroy()} is called once object
 * is no more required.
 *
 * @author shashwat
 * 
 */
public interface Initializable extends CanDestroy
{
    /**
     * Initialization
     */
    public void init();

    abstract class LifeCycle
    {
        public static <T> void initialize(T t)
        {
            if(t instanceof Initializable)
            {
                ((Initializable)t).init();
            }
        }

        public static <T> void destroy(T t)
        {
            if(t instanceof Initializable)
            {
                ((Initializable)t).destroy();
            }
        }

        private LifeCycle()
        {
        }
    }

}
