package flipkart.platform.workflow.utils;

import com.google.common.eventbus.EventBus;

/**
 * User: shashwat
 * Date: 28/07/12
 */
public interface Subscriber
{
    void subscribe(EventBus eventBus);

    public class Utils
    {
        public static <S> void register(EventBus eventBus, S s)
        {
            if (s instanceof Subscriber)
            {
                ((Subscriber) s).subscribe(eventBus);
            }
        }
    }
}
