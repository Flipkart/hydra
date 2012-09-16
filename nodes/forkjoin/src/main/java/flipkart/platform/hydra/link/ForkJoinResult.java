package flipkart.platform.hydra.link;

import java.util.Set;
import flipkart.platform.hydra.utils.UnModifiableMap;
import flipkart.platform.hydra.utils.UnModifiableSet;

/**
 * User: shashwat
 * Date: 11/08/12
 */
public class ForkJoinResult<I, O>
{
    public final boolean predicateResult;
    public final UnModifiableMap<I, O> finishedForks;
    public final UnModifiableSet<I> unFinishedForks;

    public ForkJoinResult(boolean predicateResult, UnModifiableMap<I, O> finishedForks,
        UnModifiableSet<I> unFinishedForks)
    {
        this.predicateResult = predicateResult;

        this.finishedForks = finishedForks;
        this.unFinishedForks = unFinishedForks;
    }

    @Override
    public String toString()
    {
        return "ForkJoinResult{" +
            "predicateResult=" + predicateResult +
            ", finishedForks=" + finishedForks +
            ", unFinishedForks=" + unFinishedForks +
            '}';
    }
}
