package flipkart.platform.node.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import com.ning.http.client.*;
import flipkart.platform.workflow.job.JobFactory;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AbstractNode;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.HQueue;
import flipkart.platform.workflow.queue.MessageCtx;

/**
 * User: shashwat
 * Date: 28/07/12
 */
public class HttpNode<I, O> extends AbstractNode<I, O, HttpJob<I, O>>
{
    private final AsyncHttpClient client;

    public HttpNode(String name, ExecutorService executorService, HQueue<I> queue, HttpNodeConfiguration config,
        JobFactory<? extends HttpJob<I, O>> httpJobFactory, RetryPolicy<I> retryPolicy, Link<O> link)
    {
        super(name, executorService, queue, retryPolicy, httpJobFactory, link);

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
        super.shutdownResources(awaitTermination);
        client.close();
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new HttpWorker());
    }

    /**
     * Already executes in HTTP callback context
     */
    private class HttpWorker extends WorkerBase
    {
        @Override
        protected void execute(final HttpJob<I, O> httpJob)
        {
            final MessageCtx<I> messageCtx = queue.read();

            final I i = messageCtx.get();

            try
            {
                final Request request = httpJob.buildRequest(client, i);
                client.executeRequest(request, new AsyncCompletionHandler<O>()
                {
                    @Override
                    public O onCompleted(Response response) throws Exception
                    {
                        final O o = httpJob.buildResponse(i, response);
                        ackMessage(messageCtx, o);
                        return o;
                    }

                    @Override
                    public void onThrowable(Throwable t)
                    {
                        retryMessage(httpJob, messageCtx, t);
                    }
                });
            }
            catch (IOException e)
            {
                retryMessage(httpJob, messageCtx, e);
            }
            catch (RuntimeException e)
            {
                discardMessage(httpJob, messageCtx, e);
            }
        }

        private void ackMessage(MessageCtx<I> messageCtx, O output)
        {
            if (output != null)
            {
                sendForward(output);
            }
            messageCtx.ack();
        }
        //
        //private void retryMessage(HttpJob<I, O> httpJob, MessageCtx<I> messageCtx, Throwable t)
        //{
        //    if (!retryPolicy.retry(HttpNode.this, messageCtx))
        //    {
        //        discardMessage(httpJob, messageCtx, t);
        //    }
        //    // TODO: log
        //}
        //
        //private void discardMessage(HttpJob<I, O> httpJob, MessageCtx<I> messageCtx, Throwable t)
        //{
        //    httpJob.failed(messageCtx.get(),
        //        new NoMoreRetriesException("No more retries available, cause " + t.getMessage(), t));
        //    messageCtx.discard(MessageCtx.DiscardAction.REJECT);
        //    // TODO: log
        //}
    }
}
