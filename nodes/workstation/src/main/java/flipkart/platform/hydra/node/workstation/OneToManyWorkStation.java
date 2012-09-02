package flipkart.platform.hydra.node.workstation;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.OneToManyJob;
import flipkart.platform.hydra.node.AbstractNode;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.queue.HQueue;

/**
 * A {@link AbstractNode} that executes {@link OneToManyJob}
 *
 * @author shashwat
 */
public class OneToManyWorkStation<I, O> extends WorkStationBase<I, O, OneToManyJob<I, O>>
{
    public OneToManyWorkStation(String name, ExecutorService executorService, HQueue<I> queue,
        RetryPolicy<I> retryPolicy, JobFactory<? extends OneToManyJob<I, O>> jobFactory)
    {
        super(name, executorService, queue, retryPolicy, jobFactory);
    }

    @Override
    protected void scheduleJob()
    {
        executeWorker(new OneToManyWorker(newJobExecutionContext(), queue.read()));
    }

    private static class OneToManyWorker<I, O> implements Runnable
    {
        private final JobExecutionContext<I, O, OneToManyJob<I, O>> jobExecutionContext;
        private final MessageCtx<I> messageCtx;

        public OneToManyWorker(JobExecutionContext<I, O, OneToManyJob<I, O>> jobExecutionContext,
            MessageCtx<I> messageCtx)
        {
            this.jobExecutionContext = jobExecutionContext;
            this.messageCtx = messageCtx;
        }

        @Override
        public void run()
        {
            final I i = messageCtx.get();

            final OneToManyJob<I, O> job = jobExecutionContext.begin();
            if (job != null)
            {
                try
                {
                    final Collection<O> list = job.execute(i);
                    for (final O o : list)
                    {
                        jobExecutionContext.submitResponse(o);
                    }
                    jobExecutionContext.succeeded(job, messageCtx);
                }
                catch (Exception ex)
                {
                    jobExecutionContext.failed(job, messageCtx, ex);
                }
                finally
                {
                    jobExecutionContext.end(job);
                }
            }
        }
    }
}
