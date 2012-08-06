package flipkart.platform.workflow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import flipkart.platform.node.builder.WSBuilder;
import flipkart.platform.node.jobs.ManyToManyJob;
import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.hydra.link.Selector;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.utils.UnModifiableMap;

public class Showcase
{
    public static class Job1 extends JobBase<String> implements
        OneToOneJob<String, Integer>
    {
        @Override
        public Integer execute(String i)
        {
            final int num = Integer.parseInt(i) + 1;
            System.out.println("Job1 int " + num);
            return num;
        }
    }

    public static class Job2 extends JobBase<Integer> implements
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

    public static class EvenJob extends JobBase<String> implements
        OneToOneJob<String, Void>
    {
        @Override
        public Void execute(String i)
        {
            System.out.println("Even " + i);
            return null;
        }
    }

    public static class OddJob extends JobBase<String> implements
        OneToOneJob<String, Void>
    {
        @Override
        public Void execute(String i)
        {
            System.out.println("Odd " + i);
            return null;
        }
    }

    public static class MultiJob extends JobBase<Integer> implements
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
                WSBuilder.withM2MJob(MultiJob.class).withBatch(3, 3).withThreadExecutor(2).withMaxAttempts(2).build();

            final Node<Integer, String> ws2 =
                WSBuilder.withO2OJob(Job2.class).withMaxAttempts(2).withSelector(new MySelector())
                    .build();

            ws1.append(wsM);
            wsM.append(ws2);

            final Node<String, Void> evenNode = WSBuilder.withO2OJob(EvenJob.class).build();
            final Node<String, Void> oddNode = WSBuilder.withO2OJob(OddJob.class).build();

            ws2.append(evenNode);
            ws2.append(oddNode);

            final long start = System.nanoTime();
            for (int i = 1; i <= 100; ++i)
            {
                ws1.accept(String.valueOf(i));
            }
            ws1.shutdown(true);

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
