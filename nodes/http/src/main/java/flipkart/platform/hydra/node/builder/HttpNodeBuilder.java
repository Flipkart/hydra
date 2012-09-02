package flipkart.platform.hydra.node.builder;

import java.util.concurrent.TimeUnit;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.HttpJob;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.http.HttpNode;
import flipkart.platform.hydra.node.http.HttpNodeConfiguration;
import flipkart.platform.hydra.utils.DefaultJobFactory;

/**
 * User: shashwat
 * Date: 05/08/12
 */
public class HttpNodeBuilder<I, O> extends AbstractWorkStationBuilder<I, O>
{
    private long connectionTimeoutInMs = TimeUnit.SECONDS.toMillis(5);
    private long requestTimeoutInMs = TimeUnit.SECONDS.toMillis(5);
    private boolean followRedirects = true;
    private int maximumNumberOfRedirects = 5;
    private final JobFactory<? extends HttpJob<I, O>> jobFactory;

    public HttpNodeBuilder(String name, JobFactory<? extends HttpJob<I, O>> jobFactory)
    {
        super(name);
        this.jobFactory = jobFactory;
    }

    public static <I, O> HttpNodeBuilder<I, O> with(Class<? extends HttpJob<I,
        O>> httpJob) throws NoSuchMethodException
    {
        return withHttpJob(httpJob.getSimpleName(), DefaultJobFactory.create(httpJob));
    }

    private static <I, O> HttpNodeBuilder<I, O> withHttpJob(String name, JobFactory<? extends HttpJob<I,
        O>> jobFactory)
    {
        return new HttpNodeBuilder<I, O>(name, jobFactory);
    }

    public void setConnectionTimeoutInMs(long connectionTimeoutInMs)
    {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
    }

    public void setRequestTimeoutInMs(long requestTimeoutInMs)
    {
        this.requestTimeoutInMs = requestTimeoutInMs;
    }

    public void setFollowRedirects(boolean followRedirects)
    {
        this.followRedirects = followRedirects;
    }

    public void setMaximumNumberOfRedirects(int maximumNumberOfRedirects)
    {
        this.maximumNumberOfRedirects = maximumNumberOfRedirects;
    }

    @Override
    public Node<I, O> build()
    {
        final HttpNodeConfiguration configuration =
            new HttpNodeConfiguration(connectionTimeoutInMs, requestTimeoutInMs, followRedirects,
                maximumNumberOfRedirects);
        return new HttpNode<I, O>(name, executorService, queue, configuration, jobFactory, retryPolicy);
    }
}
