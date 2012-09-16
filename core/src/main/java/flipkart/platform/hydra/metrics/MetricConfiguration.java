package flipkart.platform.hydra.metrics;

/**
 * User: shashwat
 * Date: 03/09/12
 */
public class MetricConfiguration
{
    private static MetricConfiguration ourInstance = new MetricConfiguration("flipkart.platform", "hydra");

    public static MetricConfiguration getInstance()
    {
        return ourInstance;
    }

    public static void initialize(String appGroupName, String appName)
    {
        ourInstance = new MetricConfiguration(appGroupName, appName);
    }

    private final String appGroupName, appName;

    private MetricConfiguration(String appGroupName, String appName)
    {
        this.appGroupName = appGroupName;
        this.appName = appName;
    }

    public String getAppGroupName()
    {
        return appGroupName;
    }

    public String getAppName()
    {
        return appName;
    }
}
