package flipkart.platform.workflow.queue;

/**
 * User: shashwat
 * Date: 23/07/12
 */
public interface Queue<I>
{
    void enqueue(I i);

    MessageCtx<I> read();

    MessageCtxBatch<I> read(int limit);

    int size();

    boolean isEmpty();
}
