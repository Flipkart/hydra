package flipkart.platform.hydra.link;

import flipkart.platform.hydra.utils.UnModifiableMap;
import flipkart.platform.hydra.utils.UnModifiableSet;

/**
 * User: shashwat
 * Date: 11/08/12
 */
public class NodeJoinResult<I, O>
{
    public final boolean predicateResult;
    public final I i;
    public final UnModifiableMap<String, O> finishedForks;
    public final UnModifiableSet<String> unfinishedForks;

    public NodeJoinResult(boolean predicateResult, I i, UnModifiableMap<String, O> finishedForks,
        UnModifiableSet<String> unfinishedForks)
    {
        this.predicateResult = predicateResult;
        this.i = i;
        this.finishedForks = finishedForks;
        this.unfinishedForks = unfinishedForks;
    }
}
