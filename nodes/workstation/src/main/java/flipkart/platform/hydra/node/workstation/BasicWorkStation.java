package flipkart.platform.hydra.node.workstation;

import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.BasicJob;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.utils.NoRetryPolicy;

/**
 * A {@link AbstractNode} that accepts and executes {@link flipkart.platform.hydra.job.BasicJob}.
 *
 * @author shashwat
 */

public class BasicWorkStation<I, O> extends WorkStationBase<I, O, BasicJob<I, O>>
{
    public BasicWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        JobFactory<? extends BasicJob<I, O>> basicJobJobFactory)
    {
        super(name, executorService, queue, new NoRetryPolicy<I>(), basicJobJobFactory);
    }

    @Override
    protected void scheduleJob()
    {
        executeWorker(new BasicWorker(newJobExecutionContext(), queue.read()));
    }

    private static class BasicWorker<I, O> implements Runnable
    {
        private final JobExecutionContext<I, O, BasicJob<I, O>> jobExecutionContext;
        private final MessageCtx<I> messageCtx;

        public BasicWorker(JobExecutionContext<I, O, BasicJob<I, O>> jobExecutionContext, MessageCtx<I> messageCtx)
        {
            this.jobExecutionContext = jobExecutionContext;
            this.messageCtx = messageCtx;
        }

        @Override
        public void run()
        {
            final BasicJob<I, O> job = jobExecutionContext.begin();
            job.execute(messageCtx, jobExecutionContext);
        }

    }

}
