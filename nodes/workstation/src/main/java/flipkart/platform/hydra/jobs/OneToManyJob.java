package flipkart.platform.hydra.jobs;

import java.util.Collection;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.job.Job;

/**
 * Job object that is expected to generate one or more jobs for execution, like
 * a multiplexer
 * 
 * @author shashwat
 * 
 * @param <I>
 *            Input job description type
 * @param <O>
 *            Output job descritpion type
 */
public interface OneToManyJob<I, O> extends Job<I>
{
    /**
     * Called to execute job. On unrecoverable exceptions (runtime or otherwise)
     * or failure , expected to throw {@link flipkart.platform.hydra.job.ExecutionFailureException} for
     * retrying.
     *
     * @param i
     *            job descriptions
     * @return One or more jobs for next level processing
     *
     * @throws flipkart.platform.hydra.job.ExecutionFailureException
     */

    public Collection<O> execute(I i) throws ExecutionFailureException;
}
