package flipkart.platform.hydra.link;

import com.google.common.base.Predicate;
import flipkart.platform.hydra.link.ForkUnit;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public interface JoinPredicate<I, S> extends Predicate<ForkUnit<I, S>>
{
}
