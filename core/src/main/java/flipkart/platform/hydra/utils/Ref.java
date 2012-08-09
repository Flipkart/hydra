package flipkart.platform.hydra.utils;

import javax.annotation.Nullable;

/**
* User: shashwat
* Date: 07/08/12
*/
public class Ref<I>
{
    private final I i;

    public Ref(@Nullable I i)
    {
        this.i = i;
    }

    public Ref()
    {
        this(null);
    }

    public I getI()
    {
        return i;
    }
}
