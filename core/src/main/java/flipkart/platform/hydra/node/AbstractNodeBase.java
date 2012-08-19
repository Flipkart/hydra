package flipkart.platform.hydra.node;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import flipkart.platform.hydra.utils.RunState;

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
    private final String name;
    protected final RunState runState = new RunState();

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
        if (runState.shuttingDown())
        {
            // loop and check if there are no jobs in the queue and no workers executing any job
            while (awaitTermination && !isDone())
            {
                Thread.sleep(10);
            }

            shutdownResources(awaitTermination);
            runState.shutdown();

            for (NodeEventListener<O> eventListener : eventListeners)
            {
                eventListener.onShutdown(this, awaitTermination);
            }
        }
        else
        {
            throw new RuntimeException("Shutdown already in progress for node: " + getName());
        }
    }

    @Override
    public boolean isShutdown()
    {
        return runState.isShutdown();
    }

    protected final void validateState()
    {
        if (isDone())
        {
            throw new RuntimeException("accept() called after shutdown()");
        }
    }

    // Can override completely
    public boolean isDone()
    {
        return (!runState.isActive());
    }

    // Can override completely
    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
    }

    protected void sendForward(O o)
    {
        for (NodeEventListener<O> eventListener : eventListeners)
        {
            eventListener.onNewMessage(AbstractNodeBase.this, o);
        }
    }


    protected abstract void acceptMessage(I i);

}
