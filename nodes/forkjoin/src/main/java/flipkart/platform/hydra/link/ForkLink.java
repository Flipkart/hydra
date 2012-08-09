package flipkart.platform.hydra.link;

import java.util.Collection;
import flipkart.platform.hydra.link.AbstractLink;
import flipkart.platform.hydra.link.Selector;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.utils.UnModifiableMap;

/**
* User: shashwat
* Date: 08/08/12
*/
public class ForkLink<I> extends AbstractLink<Collection<I>, I>
{
    public ForkLink(Selector<I> selector)
    {
        super(selector);
    }

    @Override
    public boolean forward(Collection<I> collection)
    {
        for (I i : collection)
        {
            send(i);
        }
        return true;
    }
}
