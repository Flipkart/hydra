package flipkart.platform.hydra.metrics;

import java.util.concurrent.TimeUnit;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;

/**
 * User: shashwat
 * Date: 20/07/12
 */
public class NodeMetrics
{
    public static enum Result
    {
        FAILED, SUCCEEDED
    }

    private long jobStartTime = 0;

    private final Counter activeJobCounter;
    private final Timer jobProcessingTime;
    private final Timer messageWaitTimer;
    private Counter messageSuccessCounter;
    private Counter messageFailureCounter;
    private final Histogram messageAttemptsHistogram;
    private Meter messageProcessedMeter;

    public NodeMetrics(String scope)
    {
        final MetricConfiguration instance = MetricConfiguration.getInstance();
        final String team = instance.getAppGroupName() + "." + instance.getAppName();

        this.jobProcessingTime = Metrics
            .newTimer(new MetricName(team, "node", "jobs_processing_time", scope), TimeUnit.MILLISECONDS,
                TimeUnit.MINUTES);

        this.activeJobCounter = Metrics.newCounter(new MetricName(team, "node", "jobs_active", scope));

        this.messageSuccessCounter = Metrics.newCounter(new MetricName(team, "node", "messages_success_count", scope));
        this.messageFailureCounter = Metrics.newCounter(new MetricName(team, "node", "messages_failure_count", scope));

        this.messageProcessedMeter = Metrics
            .newMeter(new MetricName(team, "node", "messages_processing_rate", scope), "messages_processed",
                TimeUnit.SECONDS);

        this.messageWaitTimer = Metrics
            .newTimer(new MetricName(team, "node", "message_queue_wait_time", scope), TimeUnit.MILLISECONDS,
                TimeUnit.MINUTES);

        this.messageAttemptsHistogram =
            Metrics.newHistogram(new MetricName(team, "node", "message_retry_attempt_count", scope));
    }

    public void reportJobStart()
    {
        this.jobStartTime = System.currentTimeMillis();
        activeJobCounter.inc();
    }

    public void reportJobEnd()
    {
        if (jobStartTime != 0)
        {
            activeJobCounter.dec();
            jobProcessingTime.update(System.currentTimeMillis() - jobStartTime, TimeUnit.MILLISECONDS);
        }
    }

    public void reportMessageProcessed(Result succeeded)
    {
        messageProcessedMeter.mark();

        switch (succeeded)
        {
        case FAILED:
            messageFailureCounter.inc();
            break;
        case SUCCEEDED:
            messageSuccessCounter.inc();
            break;
        }
    }

    public void reportMessageRetryAttempts(int attempt)
    {
        messageAttemptsHistogram.update(attempt);
    }

    public void reportMessageProcessingTime(long createdTimestamp)
    {
        if (jobStartTime != 0)
        {
            messageWaitTimer.update(jobStartTime - createdTimestamp, TimeUnit.MILLISECONDS);
        }
    }
}
