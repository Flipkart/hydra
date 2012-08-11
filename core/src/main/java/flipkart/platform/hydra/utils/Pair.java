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

    public static <I, O> Pair<I, O> of(I i, O o)
    {
        return new Pair<I, O>(i, o);
    }

    @Override
    public String toString()
    {
        return "Pair{" +
            "first=" + first +
            ", second=" + second +
            '}';
    }
}
