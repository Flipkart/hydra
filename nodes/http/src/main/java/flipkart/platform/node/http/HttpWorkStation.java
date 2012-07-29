package flipkart.platform.node.http;

import java.io.IOException;
import java.util.concurrent.Executors;
import com.ning.http.client.*;
import flipkart.platform.node.workstation.AbstractWorkStation;
import flipkart.platform.workflow.node.RetryPolicy;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.utils.DefaultRetryPolicy;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public abstract class HttpWorkStation<I, O> extends AbstractWorkStation<I, O>
{
    private final AsyncHttpClient client;
    private final HttpRequestBuilder<I> requestBuilder;
    private final HttpNodeOutputBuilder<I, O> httpNodeOutputBuilder;

    protected HttpWorkStation(String name, int numThreads, int maxAttempts, HttpNodeConfiguration config,
        HttpRequestBuilder<I> requestBuilder, HttpNodeOutputBuilder<I, O> httpNodeOutputBuilder)
    {
        this(name, numThreads, new DefaultRetryPolicy<I, O>(maxAttempts), config, requestBuilder,
            httpNodeOutputBuilder);
    }

    protected HttpWorkStation(String name, int numThreads, RetryPolicy<I, O> retryPolicy, HttpNodeConfiguration config,
        HttpRequestBuilder<I> requestBuilder, HttpNodeOutputBuilder<I, O> httpNodeOutputBuilder)
    {
        // TODO: default thread policy with custom name
        super(name, numThreads, retryPolicy, Executors.defaultThreadFactory());
        this.requestBuilder = requestBuilder;
        this.httpNodeOutputBuilder = httpNodeOutputBuilder;
        client = new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
            .setConnectionTimeoutInMs((int) config.getConnectionTimeoutInMs())
            .setRequestTimeoutInMs((int) config.getRequestTimeoutInMs())
            .setFollowRedirects(config.getFollowRedirects())
            .setMaximumNumberOfRedirects(config.getMaximumNumberOfRedirects())
            .build());
    }

    @Override
    protected void scheduleWorker()
    {
        executeWorker(new HttpWorker());
    }

    protected abstract void putEntity(O o);

    private class HttpWorker extends WorkerBase
    {
        @Override
        protected void execute()
        {
            final MessageCtx<I> messageCtx = queue.read();

            final Request request = requestBuilder.build(client, messageCtx.get());
            try
            {
                client.executeRequest(request, new AsyncCompletionHandler<O>()
                {
                    @Override
                    public O onCompleted(Response response) throws Exception
                    {
                        final O o = httpNodeOutputBuilder.build(messageCtx.get(), response);
                        putEntity(o);
                        messageCtx.ack();
                        return o;
                    }

                    @Override
                    public void onThrowable(Throwable t)
                    {
                        super.onThrowable(t);
                        // TODO: retry on throw
                        // messageCtx.retry(maxAttempts);
                    }
                });
            }
            catch (IOException e)
            {
                //TODO
            }

        }
    }

}
