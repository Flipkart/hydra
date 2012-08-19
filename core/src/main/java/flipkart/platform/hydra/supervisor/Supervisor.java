package flipkart.platform.hydra.supervisor;

import java.util.Set;
import flipkart.platform.hydra.traits.HasIdentity;
import flipkart.platform.hydra.utils.UnModifiableCollection;
import flipkart.platform.hydra.utils.UnModifiableSet;

/**
 * User: shashwat
 * Date: 15/08/12
 */
public interface Supervisor extends HasIdentity
{
    void supervise(Supervisor child);

    void unsupervise(Supervisor child);

    void addParent(Supervisor parent);

    void removeParent(Supervisor parent);

    UnModifiableCollection<Supervisor> getPredecessors();

    UnModifiableCollection<Supervisor> getSuccessors();
    
    boolean isIndependent();

    boolean isShutdown();

    boolean tryShutdown(boolean awaitTermination);
}
