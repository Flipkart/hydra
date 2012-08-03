package flipkart.platform.node.http;

import flipkart.platform.workflow.job.JobFactory;

/**
 * User: shashwat
 * Date: 02/08/12
 */
public interface HttpJobFactory<I, O> extends JobFactory<HttpJob<I, O>>
{
}
