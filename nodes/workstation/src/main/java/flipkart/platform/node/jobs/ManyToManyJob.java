package flipkart.platform.node.jobs;

import java.util.Collection;
import java.util.List;
import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.Job;

/**
 * Job Object that consumes more than one job descriptions and produces one or
 * more job descriptions (possibly not equal to input job descriptions) for
 * further processing.
 * 
 * @author shashwat
 * 
 * @param <I>
 *            Input Job description type
 * @param <O>
 *            Output Job description type
 */
public interface ManyToManyJob<I, O> extends Job<I>
{
    /**
     * Called to execute job. On unrecoverable exceptions (runtime or otherwise)
     * or failure , expected to throw {@link flipkart.platform.workflow.job.ExecutionFailureException} for
     * retrying.
     *
     * @param jobList
     *            One or more jobs descriptions
     * @return One or more jobs for next level processing
     *
     * @throws flipkart.platform.workflow.job.ExecutionFailureException
     */
    public Collection<O> execute(List<I> jobList)
            throws ExecutionFailureException;
}
