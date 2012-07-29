package flipkart.platform.node.aop;

import java.util.concurrent.TimeUnit;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.annotation.Timed;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import flipkart.platform.node.workstation.WorkStation;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

/**
 * User: shashwat
 * Date: 20/07/12
 */
@Aspect
public class JobProcessTime
{
    // TODO: Add shutdown hook to shutdown metrics

    @Around("target(worker) && execution(@com.yammer.metrics.annotation.Timed * * (..)) && @annotation(annotation)")
    public Object measureExecTime(ProceedingJoinPoint thisJoinPoint, WorkStation.Worker worker,
        Timed annotation) throws
        Throwable
    {
        final String group = annotation.group().isEmpty() ? "nodes" : annotation.group();
        final String type = annotation.type().isEmpty() ? worker.getName() : annotation.type();
        final String name = annotation.name().isEmpty() ? Thread.currentThread().getName() : annotation.name();

        final MetricName metricName = new MetricName(group, type, name);

        final Timer timer = Metrics.newTimer(metricName,
            annotation.durationUnit() == null ?
                TimeUnit.MILLISECONDS : annotation.durationUnit(),
            annotation.rateUnit() == null ?
                TimeUnit.SECONDS : annotation.rateUnit());

        final TimerContext timerContext = timer.time();
        try
        {
            return thisJoinPoint.proceed();
        }
        finally
        {
            timerContext.stop();
        }
    }

}
