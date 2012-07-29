package flipkart.platform.workflow;

import java.util.Map;
import java.util.Map.Entry;

import flipkart.platform.node.Nodes;
import flipkart.platform.node.jobs.OneToOneJob;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.junction.AnyJunction;
import flipkart.platform.workflow.node.junction.AnyJunction.Isolation;
import flipkart.platform.workflow.node.junction.AnyJunction.Selector;

public class JunctionShowcase
{
    // dummy jobs which just prints input
    public static class Job1 extends JobBase<String> implements
        OneToOneJob<String, String>
    {
        @Override
        public String execute(String i)
        {
            System.out.println("Job1 value:" + i);
            return i;
        }
    }

    public static class Job2 extends JobBase<String> implements
            OneToOneJob<String, String>
    {
        @Override
        public String execute(String i)
        {
            System.out.println("Job2 value:" + i);
            return i;
        }
    }

    public static class Job3 extends JobBase<String> implements
            OneToOneJob<String, String>
    {
        @Override
        public String execute(String i)
        {
            System.out.println("Job3 value:" + i);
            return i;
        }
    }

    // pass to all out nodes
    public static class MySelector<T> implements Selector<T>
    {

        @Override
        public void select(AnyNode<?, T> node, T i,
                Map<String, AnyNode<?, ?>> fromNodes,
                Map<String, AnyNode<?, ?>> toNodes)
        {
            {
                for (Entry<String, AnyNode<?, ?>> n : toNodes.entrySet())
                    n.getValue().acceptAny(i);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException
    {
        try
        {
            // AnyNode
            final AnyNode<?, String> a = Nodes.newO2ONode("Job1", 2, 1,
                Job1.class).anyNode(String.class, String.class);

            final AnyNode<?, String> b = Nodes.newO2ONode("Job2", 2, 1,
                    Job2.class).anyNode(String.class, String.class);

            final AnyNode<?, ?> c = Nodes.newO2ONode("Job3", 2, 1, Job3.class)
                    .anyNode(String.class, String.class);

            final AnyJunction junction = new AnyJunction("junction",
                    Isolation.NONE);

            MySelector<String> selector = new MySelector<String>();
            junction.from(selector, a);
            junction.from(selector, b);
            junction.to(c);

            a.acceptAny("abc");
            a.shutdown(true);

            b.acceptAny("def"); // will give error, node already closed
            b.shutdown(true);
        }
        catch (Exception e)
        {
            System.out.println("Exception caught" + e);
            e.printStackTrace();
        }
    }
}
