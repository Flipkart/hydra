package flipkart.platform.hydra.utils;

import com.yammer.metrics.core.*;
import com.yammer.metrics.util.RatioGauge;

/**
 * User: shashwat
 * Date: 09/08/12
 */
public class Measure extends RatioGauge
{
    private final Meter meterNum, meterDenominator;

    public Measure( Meter meterNum, Meter meterDenominator)
    {
        this.meterNum = meterNum;
        this.meterDenominator = meterDenominator;
    }

    @Override
    protected double getNumerator()
    {
        return meterNum.oneMinuteRate();
    }

    @Override
    protected double getDenominator()
    {
        return meterDenominator.oneMinuteRate();
    }
}
