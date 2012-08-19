package flipkart.platform.hydra.node;

import flipkart.platform.hydra.supervisor.AbstractSupervisor;
import flipkart.platform.hydra.supervisor.Supervisor;
import flipkart.platform.hydra.supervisor.SupervisorFactory;

/**
 * User: shashwat
 * Date: 15/08/12
 */
public class NodeSupervisor extends AbstractSupervisor<Node>
{
    public static final SupervisorFactory<Node> factory = new SupervisorFactory<Node>()
    {
        @Override
        public Supervisor newSupervisor(Node node)
        {
            return new NodeSupervisor(node);
        }
    };

    private final String name;

    public NodeSupervisor(Node node)
    {
        super(node);
        this.name = node.getName() + "-supervisor";
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    protected boolean shutdownResource(Node node, boolean awaitTermination)
    {
        try
        {
            node.shutdown(awaitTermination);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        return true;
    }
}
