package flipkart.platform.hydra.job;

import flipkart.platform.hydra.traits.CanFail;

/**
 * Marker interface that identifies jobs that can fail
 * @param <I>
 */
public interface Job<I> extends CanFail<I>
{
}
