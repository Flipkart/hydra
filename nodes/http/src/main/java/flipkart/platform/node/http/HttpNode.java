package flipkart.platform.node.http;

import java.io.IOException;
import com.ning.http.client.*;
import flipkart.platform.workflow.node.Node;

/**
 * User: shashwat
 * Date: 28/07/12
 */
public abstract class HttpNode<I, O> implements Node<I, O>
{
    private final String name;
    private final AsyncHttpClient client;
    private final HttpRequestBuilder<I> requestBuilder;
    private final HttpNodeOutputBuilder<I, O> httpNodeOutputBuilder;

    public HttpNode(String name, HttpNodeConfiguration config, HttpRequestBuilder<I> requestBuilder,
        HttpNodeOutputBuilder<I, O> httpNodeOutputBuilder)
    {
        this.name = name;
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
    public String getName()
    {
        return name;
    }

    @Override
    public void accept(final I i)
    {
        final Request request = requestBuilder.build(client, i);
        try
        {
            client.executeRequest(request, new AsyncCompletionHandler<O>()
            {
                @Override
                public O onCompleted(Response response) throws Exception
                {
                    final O o = httpNodeOutputBuilder.build(i, response);
                    putEntity(o);
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

    @Override
    public void shutdown(boolean awaitTermination) throws InterruptedException
    {
        client.close();
    }

    protected abstract void putEntity(O o);
}
