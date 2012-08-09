package flipkart.platform.hydra.utils;

/**
 * User: shashwat
 * Date: 07/08/12
 */
public class Pair<I, O>
{
    public final I first;
    public final O second;

    public Pair(I first, O second)
    {
        this.first = first;
        this.second = second;
    }

    public static <I, O> Pair<I, O> from(I i, O o)
    {
        return new Pair<I, O>(i, o);
    }
}
