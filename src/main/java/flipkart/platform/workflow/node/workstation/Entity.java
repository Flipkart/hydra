package flipkart.platform.workflow.node.workstation;

/**
 * Internal. Job meta information
 * 
 * @author shashwat
 * 
 * @param <I>
 *            Job description to wrap
 */
class Entity<I>
{
    public final I i;
    public final byte attempt;

    public Entity(I i)
    {
        this.i = i;
        this.attempt = 1;
    }

    public Entity(Entity<I> e)
    {
        this.i = e.i;
        this.attempt = (byte) (1 + e.attempt);
    }

    public static <I> Entity<I> wrap(I i)
    {
        return new Entity<I>(i);
    }

    public static <I> Entity<I> from(Entity<I> e)
    {
        return new Entity<I>(e);
    }
}
