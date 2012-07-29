package flipkart.platform.node.jobs;

import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.Job;

/**
 * Job object that executes and generate a single job.
 * 
 * @author shashwat
 * 
 * @param <I>
 *            Input job description type
 * @param <O>
 *            Output job description type
 */

public interface OneToOneJob<I, O> extends Job<I>
{
    /**
     * Called to execute job. On unrecoverable exceptions (runtime or otherwise)
     * or failure , expected to throw {@link flipkart.platform.workflow.job.ExecutionFailureException} for
     * retrying.
     *
     * @param i
     *            One or more jobs descriptions
     * @return job for next level processing
     *
     * @throws flipkart.platform.workflow.job.ExecutionFailureException
     */

    public O execute(I i) throws ExecutionFailureException;
}
