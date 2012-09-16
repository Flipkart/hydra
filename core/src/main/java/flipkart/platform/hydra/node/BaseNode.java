package flipkart.platform.hydra.node;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import flipkart.platform.hydra.utils.RunState;

/**
 * A {@link Node} base abstraction that takes care of node's name and event management. It is recommended to extend
 * this base class when implementing new {@link Node} type
 *
 * @param <I>
 *     Input job description type
 * @param <O>
 *     Output job description type
 * @author shashwat
 */
public abstract class BaseNode<I, O> implements Node<I, O>
{
    private final String name;
    protected final RunState runState = new RunState();

    protected final Queue<NodeEventListener<O>> eventListeners = new ConcurrentLinkedQueue<NodeEventListener<O>>();

    protected BaseNode(String name)
    {
        this.name = name;
    }

    @Override
    public String getIdentity()
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
            throw new RuntimeException("Shutdown already in progress for node: " + getIdentity());
        }
    }

    @Override
    public boolean isShutdown()
    {
        return runState.isShutdown();
    }

    protected final void validateState()
    {
        if (!runState.isActive())
        {
            throw new RuntimeException("accept() called after shutdown()");
        }
    }

    /**
     * @return <code>true</code> if all messages are consumed and it is safe to call {@link #shutdown(boolean)}
     */
    public boolean isDone()
    {
        return (!runState.isActive());
    }

    /**
     * helper method to notify listeners of the availability of new output message
     * @param o the output message that needs to be sent
     */
    protected void sendForward(O o)
    {
        for (NodeEventListener<O> eventListener : eventListeners)
        {
            eventListener.onNewMessage(BaseNode.this, o);
        }
    }

    /**
     * To be implemented by the derived classes to shutdown/destroy the resources.
     *
     * @param awaitTermination
     *     Boolean, if <code>true</code> wait for complete termination of all the resources.
     * @throws InterruptedException
     *     if Interrupted exception was thrown when waiting for termination
     */
    protected abstract void shutdownResources(boolean awaitTermination) throws InterruptedException;

    /**
     * To be implemented by the derived classes to accept the message
     *
     * @param i Message that needs to be processed by this node
     * @see Node#accept(Object) 
     */
    protected abstract void acceptMessage(I i);

}
