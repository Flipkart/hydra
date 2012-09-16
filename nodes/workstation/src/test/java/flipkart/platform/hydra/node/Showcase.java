package flipkart.platform.hydra.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import flipkart.platform.hydra.job.AbstractJob;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.link.Selector;
import flipkart.platform.hydra.node.builder.WSBuilder;
import flipkart.platform.hydra.topology.LinkTopology;
import flipkart.platform.hydra.utils.UnModifiableMap;

import static flipkart.platform.hydra.link.LinkBuilder.link;
import static flipkart.platform.hydra.link.LinkBuilder.using;

public class Showcase
{
    public static class Job1 extends AbstractJob<String> implements OneToOneJob<String, Integer>
    {
        @Override
        public Integer execute(String i)
        {
            final int num = Integer.parseInt(i) + 1;
            System.out.println("Job1 int " + num);
            return num;
        }
    }

    public static class Job2 extends AbstractJob<Integer> implements
        OneToOneJob<Integer, String>
    {
        @Override
        public String execute(Integer i)
        {
            final String str = String.valueOf(i + 1);
            System.out.println("Job2 str " + str);
            return str;
        }
    }

    public static class EvenJob extends AbstractJob<String> implements
        OneToOneJob<String, Void>
    {
        @Override
        public Void execute(String i)
        {
            System.out.println("Even " + i);
            return null;
        }
    }

    public static class OddJob extends AbstractJob<String> implements
        OneToOneJob<String, Void>
    {
        @Override
        public Void execute(String i)
        {
            System.out.println("Odd " + i);
            return null;
        }
    }

    public static class MultiJob extends AbstractJob<Integer> implements
        ManyToManyJob<Integer, Integer>
    {
        public static AtomicInteger count = new AtomicInteger();

        private int index = count.incrementAndGet();

        @Override
        public List<Integer> execute(List<Integer> jobList)
        {
            System.out.println("MultiJob list size: " + jobList.size());
            List<Integer> intList = new ArrayList<Integer>();

            for (Integer i : jobList)
            {
                intList.add(i + 1);
                System.out.println("MultiJob (" + index + ") int " + (i + 1));
            }
            return intList;
        }
    }

    public static class MySelector implements Selector<String>
    {
        @Override
        public Collection<Node<String, ?>> select(String str, UnModifiableMap<String, Node<String, ?>> nodes)
        {
            final int i = Integer.parseInt(str);
            final List<Node<String, ?>> outNodes = new ArrayList<Node<String, ?>>();
            outNodes.add(nodes.get((i % 2 == 0) ? EvenJob.class.getSimpleName()
                : OddJob.class.getSimpleName()));
            return outNodes;
        }
    }

    public static void main(String[] args) throws InterruptedException
    {
        try
        {
            final Node<String, Integer> ws1 =
                WSBuilder.withO2OJob(Job1.class).withMaxAttempts(1).build();

            final Node<Integer, Integer> wsM =
                WSBuilder.withM2MJob(MultiJob.class).withBatch(10, 10).withThreadExecutor(2).withMaxAttempts(2).build();

            final Node<Integer, String> ws2 =
                WSBuilder.withO2OJob(Job2.class).withMaxAttempts(2).build();

            final Node<String, Void> evenNode = WSBuilder.withO2OJob(EvenJob.class).build();
            final Node<String, Void> oddNode = WSBuilder.withO2OJob(OddJob.class).build();

            final LinkTopology topology = new LinkTopology();

            link(topology, ws1).to(wsM).to(ws2);
            using(topology, new MySelector()).linkFrom(ws2).to(evenNode, oddNode);

            final long start = System.nanoTime();
            for (int i = 1; i <= 100; ++i)
            {
                ws1.accept(String.valueOf(i));
            }
            //ws1.shutdown(true);

            topology.shutdown(true);

            final long end = System.nanoTime();
            System.out.print("Done, It took ");
            System.out.print(TimeUnit.MILLISECONDS.convert((end - start), TimeUnit.NANOSECONDS));
            System.out.println(" ms to complete");
        }
        catch (Exception e)
        {
            System.out.println("Exception caught" + e);
            e.printStackTrace();
        }
    }
}
