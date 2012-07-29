package flipkart.platform.node.http;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public class HttpNodeConfiguration
{
    private long connectionTimeoutInMs;
    private long requestTimeoutInMs;
    private boolean followRedirects;
    private int maximumNumberOfRedirects;

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
