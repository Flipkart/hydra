package flipkart.platform.node.http;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public class HttpNodeConfiguration
{
    private final long connectionTimeoutInMs;
    private final long requestTimeoutInMs;
    private final boolean followRedirects;
    private final int maximumNumberOfRedirects;

    public HttpNodeConfiguration(long connectionTimeoutInMs, long requestTimeoutInMs, boolean followRedirects,
        int maximumNumberOfRedirects)
    {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
        this.requestTimeoutInMs = requestTimeoutInMs;
        this.followRedirects = followRedirects;
        this.maximumNumberOfRedirects = maximumNumberOfRedirects;
    }

    public long getConnectionTimeoutInMs()
    {
        return connectionTimeoutInMs;
    }

    public long getRequestTimeoutInMs()
    {
        return requestTimeoutInMs;
    }

    public boolean getFollowRedirects()
    {
        return followRedirects;
    }

    public int getMaximumNumberOfRedirects()
    {
        return maximumNumberOfRedirects;
    }
}
