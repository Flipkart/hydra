package flipkart.platform.hydra.aop;

import java.util.concurrent.TimeUnit;
import com.sun.jdi.VirtualMachine;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.annotation.Timed;
import com.yammer.metrics.core.*;
import flipkart.platform.hydra.node.AbstractNode;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

/**
 * User: shashwat
 * Date: 20/07/12
 */
@Aspect
public class NodeMetrics
{
    @Around("target(worker) && execution(@com.yammer.metrics.annotation.Timed * * (..)) && @annotation(annotation)")
    public Object measureExecTime(ProceedingJoinPoint thisJoinPoint, AbstractNode.WorkerBase worker,
        Timed annotation) throws Throwable
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

        final Counter counter = Metrics.newCounter(new MetricName(group, type, "current_executing"));

        final TimerContext timerContext = timer.time();
        counter.inc();
        try
        {
            return thisJoinPoint.proceed();
        }
        finally
        {
            timerContext.stop();
            counter.dec();
        }
    }

}
