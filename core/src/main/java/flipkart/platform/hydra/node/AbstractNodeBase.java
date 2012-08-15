package flipkart.platform.hydra.node;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An base {@link Node} implementation
 *
 * @param <I>
 *     Input job description type
 * @param <O>
 *     Output job description type
 * @author shashwat
 */
public abstract class AbstractNodeBase<I, O> implements Node<I, O>
{
    public static enum RunState
    {
        ACTIVE,
        SHUTTING_DOWN,
        SHUTDOWN
    }

    private final String name;
    protected final AtomicReference<RunState> runState = new AtomicReference<RunState>(RunState.ACTIVE);
    
    protected final Queue<NodeEventListener<O>> eventListeners = new ConcurrentLinkedQueue<NodeEventListener<O>>();

    protected AbstractNodeBase(String name)
    {
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void addListener(NodeEventListener<O> nodeListener)
    {
        eventListeners.add(nodeListener);
    }

    @Override
    public void accept(I i)
    {
        validateState();

        acceptMessage(i);
    }

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        if (runState.compareAndSet(RunState.ACTIVE, RunState.SHUTTING_DOWN))
        {
            // loop and check if there are no jobs in the queue and no workers executing any job
            while (awaitTermination && !isDone())
            {
                Thread.sleep(10);
            }

            shutdownResources(awaitTermination);
            runState.set(RunState.SHUTDOWN);
            for (NodeEventListener<O> eventListener : eventListeners)
            {
                eventListener.onShutdown(this, awaitTermination);
            }
        }
        else
        {
            throw new RuntimeException("Shutdown already in progress");
        }
    }

    protected final void validateState()
    {
        if (runState.get() != RunState.ACTIVE)
        {
            throw new RuntimeException("accept() called after shutdown()");
        }
    }

    // Can override completely
    public boolean isDone()
    {
        return (runState.get() != RunState.ACTIVE);
    }

    // Can override completely
    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
    }

    protected abstract void acceptMessage(I i);

}
