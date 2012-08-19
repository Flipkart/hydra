package flipkart.platform.hydra.utils;

import java.util.concurrent.atomic.AtomicReference;

/**
 * User: shashwat
 * Date: 17/08/12
 */
public class RunState
{
    enum State
    {
        ACTIVE, SHUTTING_DOWN, SHUTDOWN
    }

    private AtomicReference<State> state = new AtomicReference<State>(State.ACTIVE);

    public boolean shuttingDown()
    {
        return state.compareAndSet(State.ACTIVE, State.SHUTTING_DOWN);
    }

    public boolean shutdown()
    {
        return state.compareAndSet(State.SHUTTING_DOWN, State.SHUTDOWN);
    }

    public boolean isActive()
    {
        return state.get() == State.ACTIVE;
    }

    public boolean isShutdown()
    {
        return state.get() == State.SHUTDOWN;
    }
}
