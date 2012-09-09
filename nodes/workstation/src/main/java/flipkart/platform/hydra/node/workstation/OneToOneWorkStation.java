package flipkart.platform.hydra.node.workstation;

import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;

/**
 * A {@link AbstractNode} that accepts and executes {@link OneToOneJob}.
 *
 * @author shashwat
 */

public class OneToOneWorkStation<I, O> extends WorkStationBase<I, O, OneToOneJob<I, O>>
{
    public OneToOneWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        RetryPolicy<I> retryPolicy, JobFactory<? extends OneToOneJob<I, O>> jobFactory)
    {
        super(name, executorService, queue, retryPolicy, jobFactory);
    }

    @Override
    protected void scheduleJob()
    {
        executeWorker(new OneToOneWorker(jobExecutionContextFactory, queue.read()));
    }

    private static class OneToOneWorker<I, O> implements Runnable
    {
        private final JobExecutionContextFactory<I, O, OneToOneJob<I, O>> jobExecutionContextFactory;
        private final MessageCtx<I> messageCtx;

        public OneToOneWorker(JobExecutionContextFactory jobExecutionContextFactory,
            MessageCtx messageCtx)
        {
            this.jobExecutionContextFactory = jobExecutionContextFactory;
            this.messageCtx = messageCtx;
        }

        @Override
        public void run()
        {
            final I i = messageCtx.get();
            final JobExecutionContext<I, O, OneToOneJob<I, O>> jobExecutionContext =
                jobExecutionContextFactory.newJobExecutionContext();
            final OneToOneJob<I, O> job = jobExecutionContext.getJob();

            if (job != null)
            {
                try
                {
                    final O o = job.execute(i);
                    jobExecutionContext.submitResponse(o);
                    jobExecutionContext.succeeded(messageCtx);
                }
                catch (Exception ex)
                {
                    jobExecutionContext.failed(messageCtx, ex);
                }
                finally
                {
                    jobExecutionContext.end();
                }
            }

        }
    }

}
