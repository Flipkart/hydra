package flipkart.platform.node.http;

import com.ning.http.client.Response;
import flipkart.platform.workflow.job.ExecutionFailureException;

/**
 * User: shashwat
 * Date: 29/07/12
 */
public interface HttpNodeOutputBuilder<I, O>
{
    O build(I i, Response response) throws ExecutionFailureException;
}
