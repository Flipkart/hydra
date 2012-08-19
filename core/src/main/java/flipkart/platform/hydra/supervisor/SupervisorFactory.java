package flipkart.platform.hydra.supervisor;

/**
 * User: shashwat
 * Date: 15/08/12
 */
public interface SupervisorFactory<T>
{
    Supervisor newSupervisor(T t);
}
