package flipkart.platform.node.http;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.Response;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.Job;

/**
 * User: shashwat
 * Date: 02/08/12
 */
public interface HttpJob<I, O> extends Job<I>
{
    Request buildRequest(AsyncHttpClient client, I i);
    O buildResponse(I i, Response response) throws ExecutionFailureException;

}
