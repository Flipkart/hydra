package flipkart.platform.hydra.traits;

/**
 * User: shashwat
 * Date: 11/08/12
 */
public interface Predicate<I> extends com.google.common.base.Predicate<I>
{
    boolean apply(I i);
}
