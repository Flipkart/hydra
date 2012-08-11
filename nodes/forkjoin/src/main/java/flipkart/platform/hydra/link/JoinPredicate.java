package flipkart.platform.hydra.link;

import flipkart.platform.hydra.traits.Predicate;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public interface JoinPredicate<I, O> extends Predicate<ForkUnit<I, O>>
{
    public boolean apply(ForkUnit<I, O> forkUnit);
}
