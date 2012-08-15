package flipkart.platform.hydra.link;

import java.util.concurrent.TimeUnit;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import flipkart.platform.hydra.utils.Measure;

/**
 * User: shashwat
 * Date: 12/08/12
 */
public class ForkJoinMetrics
{
    private final Meter forkMeter;
    private final Meter forkCountMeter;
    private final Timer joinTimer;

    public ForkJoinMetrics(Class<?> clazz)
    {
        forkMeter = Metrics.newMeter(new MetricName(clazz, "fork"), "fork", TimeUnit.SECONDS);
        forkCountMeter = Metrics.newMeter(new MetricName(clazz, "forkCount"), "fork", TimeUnit.SECONDS);
        Metrics.newGauge(new MetricName(clazz, "avgForks"), new Measure(forkCountMeter, forkMeter));

        joinTimer = Metrics.newTimer(new MetricName(clazz, "join"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        
    }

    void reportForks(int numForks)
    {
        forkMeter.mark();
        forkCountMeter.mark(numForks);
    }

    void reportJoin(long milliseconds)
    {
        joinTimer.update(milliseconds, TimeUnit.MILLISECONDS);
    }
}
