package flipkart.platform.hydra.node.http;

import java.util.concurrent.ExecutorService;
import com.ning.http.client.*;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.HttpJob;
import flipkart.platform.hydra.node.RetryPolicy;
import flipkart.platform.hydra.node.workstation.WorkStationBase;
import flipkart.platform.hydra.queue.HQueue;
import flipkart.platform.hydra.utils.RefCounter;

/**
 * User: shashwat
 * Date: 28/07/12
 */
public class HttpNode<I, O> extends WorkStationBase<I, O, HttpJob<I, O>>
{
    private final AsyncHttpClient client;
    private final RefCounter callbackCounter = new RefCounter(0);

    public HttpNode(String name, ExecutorService executorService, HQueue<I> queue, HttpNodeConfiguration config,
        JobFactory<? extends HttpJob<I, O>> httpJobFactory, RetryPolicy<I> retryPolicy)
    {
        super(name, executorService, queue, retryPolicy, httpJobFactory);

        client = new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
            .setConnectionTimeoutInMs((int) config.getConnectionTimeoutInMs())
            .setRequestTimeoutInMs((int) config.getRequestTimeoutInMs())
            .setFollowRedirects(config.getFollowRedirects())
            .setMaximumNumberOfRedirects(config.getMaximumNumberOfRedirects())
            .build());
    }

    @Override
    protected void shutdownResources(boolean awaitTermination) throws InterruptedException
    {
        client.close();
        super.shutdownResources(awaitTermination);
    }

    @Override
    protected void scheduleJob()
    {
        executeWorker(new HttpWorker(jobExecutionContextFactory, queue.read(), client));
    }

    @Override
    public boolean isDone()
    {
        return super.isDone() && callbackCounter.isZero();
    }

    private static class HttpWorker<I, O> implements Runnable
    {
        private final JobExecutionContextFactory<I, O, HttpJob<I, O>> jobExecutionContextFactory;
        private final MessageCtx<I> messageCtx;
        private final AsyncHttpClient client;

        public HttpWorker(JobExecutionContextFactory<I, O, HttpJob<I, O>> jobExecutionContextFactory,
            MessageCtx<I> messageCtx,
            AsyncHttpClient client)
        {
            this.jobExecutionContextFactory = jobExecutionContextFactory;
            this.messageCtx = messageCtx;
            this.client = client;
        }

        @Override
        public void run()
        {
            final I i = messageCtx.get();
            final JobExecutionContext<I, O, HttpJob<I, O>> jobExecutionContext =
                jobExecutionContextFactory.newJobExecutionContext();
            final HttpJob<I, O> job = jobExecutionContext.getJob();

            try
            {
                final Request request = job.buildRequest(client, i);
                client.executeRequest(request, new AsyncCompletionHandler<O>()
                {
                    @Override
                    public O onCompleted(Response response) throws Exception
                    {
                        final O o = job.buildResponse(i, response);
                        jobExecutionContext.submitResponse(o);
                        jobExecutionContext.succeeded(messageCtx);
                        jobExecutionContext.end();
                        return o;
                    }

                    @Override
                    public void onThrowable(Throwable t)
                    {
                        failedJob(jobExecutionContext, t);
                    }
                });
            }
            catch (Exception e)
            {
                failedJob(jobExecutionContext, e);
            }
        }

        private void failedJob(JobExecutionContext jobExecutionContext, Throwable t)
        {
            jobExecutionContext.failed(messageCtx, t);
            jobExecutionContext.end();
        }
    }
}
