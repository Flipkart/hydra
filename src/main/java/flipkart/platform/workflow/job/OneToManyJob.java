package flipkart.platform.workflow.job;

import java.util.Collection;

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
     * or failure , expected to throw {@link ExecutionFailureException} for
     * retrying.
     * 
     * @param i
     *            job descriptions
     * @return One or more jobs for next level processing
     * 
     * @throws ExecutionFailureException
     */

    public Collection<O> execute(I i) throws ExecutionFailureException;
}
