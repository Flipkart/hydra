package flipkart.platform.workflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import flipkart.platform.node.Nodes;
import flipkart.platform.node.jobs.ManyToManyJob;
import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.node.workstation.ManyToManyWorkStation;
import flipkart.platform.workflow.job.DefaultJobFactory;
import flipkart.platform.workflow.link.SelectorLink.Selector;
import flipkart.platform.workflow.link.SingleLink;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Node;

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

    public static class SingleJob extends JobBase<Integer> implements
            OneToOneJob<Integer, Integer>
    {

        @Override
        public Integer execute(Integer i)
        {
            return i + 1;
        }
    }

    public static class MySelector implements Selector<String>
    {
        @Override
        public List<Node<String, ?>> select(String str,
                Map<String, Node<String, ?>> nodes)
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
            final Node<String, Integer> ws1 = Nodes.newO2ONode("Job1", 2, 1,
                    Job1.class);

            final Node<Integer, Integer> wsM = ManyToManyWorkStation.create("MultiJob", 2, 2,
                DefaultJobFactory.create(MultiJob.class), SingleLink.<Integer>create(), 3, 3, TimeUnit.MILLISECONDS);

            final Node<Integer, String> ws2 = Nodes.newO2ONode("Job2", 2, 2,
                Job2.class, new MySelector());

            ws1.append(wsM);
            wsM.append(ws2);

            final Node<String, Void> evenNode = Nodes.newO2ONode("EvenJob", 2,
                    1, EvenJob.class);
            final Node<String, Void> oddNode = Nodes.newO2ONode("OddJob", 2, 1,
                    OddJob.class);

            ws2.append(evenNode);
            ws2.append(oddNode);

            final long start = System.nanoTime();
            for (int i = 1; i <= 100; ++i)
            {
                ws1.accept(String.valueOf(i));
            }
            ws1.shutdown(true);

            final long end = System.nanoTime();
            System.out.println("Done");
            System.out.println(TimeUnit.MILLISECONDS.convert((end - start), TimeUnit.NANOSECONDS));

            // AnyNode
            final AnyNode<?, ?> a = Nodes.newO2ONode("Job1", 2, 1, Job1.class)
                    .anyNode(String.class, Integer.class);

            final AnyNode<?, ?> b = Nodes.newO2ONode("Job2", 2, 1, Job1.class)
                    .anyNode(String.class, Integer.class);

            //b.appendAny(a); // will throw exception - wrong append

            //b.acceptAny(1); // will throw exception - wrong param
            b.shutdown(true);
        }
        catch (Exception e)
        {
            System.out.println("Exception caught" + e);
            e.printStackTrace();
        }
    }
}
