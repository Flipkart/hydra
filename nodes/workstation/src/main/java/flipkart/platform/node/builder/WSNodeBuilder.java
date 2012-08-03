package flipkart.platform.node.builder;

import flipkart.platform.workflow.job.Job;

/**
 * User: shashwat
 * Date: 03/08/12
 */
public interface WSNodeBuilder<I, O> extends NodeBuilder<I, O>
{
    WSNodeBuilder<I, O> withNumThreads(int numThreads);
}
