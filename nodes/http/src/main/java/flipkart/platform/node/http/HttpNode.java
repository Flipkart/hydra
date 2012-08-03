package flipkart.platform.node.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import com.ning.http.client.*;
import flipkart.platform.workflow.link.Link;
import flipkart.platform.workflow.node.AbstractNode;
import flipkart.platform.workflow.queue.MessageCtx;
import flipkart.platform.workflow.queue.HQueue;

/**
 * User: shashwat
 * Date: 28/07/12
 */
public class HttpNode<I, O> extends AbstractNode<I, O, HttpJob<I, O>>
{
    private final AsyncHttpClient client;

    public HttpNode(String name, HQueue<I> queue, HttpNodeConfiguration config, ExecutorService executorService,
        HttpJobFactory<I, O> httpJobFactory, Link<O> link)
    {
        super(name, queue, executorService, httpJobFactory, link);

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
        executeWorker(new WorkerBase()
        {
            @Override
            protected void execute(final HttpJob<I, O> httpJob)
            {
                final MessageCtx<I> messageCtx = queue.read();
                final I i = messageCtx.get();

                final Request request = httpJob.buildRequest(client, i);
                try
                {
                    client.executeRequest(request, new AsyncCompletionHandler<O>()
                    {
                        @Override
                        public O onCompleted(Response response) throws Exception
                        {
                            final O o = httpJob.buildResponse(i, response);
                            HttpNode.this.sendForward(o);
                            return o;
                        }

                        @Override
                        public void onThrowable(Throwable t)
                        {
                            //TODO: retry on throw
                        }
                    });
                }
                catch (IOException e)
                {
                    //TODO, wait and retry
                }
            }
        });
    }
}
