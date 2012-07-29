package flipkart.platform.node.http;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public interface HttpRequestBuilder<I>
{
    Request build(AsyncHttpClient client, I i);
}
