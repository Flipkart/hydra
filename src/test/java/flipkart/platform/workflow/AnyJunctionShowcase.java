package flipkart.platform.workflow;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import flipkart.platform.workflow.job.ExecutionFailureException;
import flipkart.platform.workflow.job.OneToManyJob;
import flipkart.platform.workflow.job.OneToOneJob;
import flipkart.platform.workflow.node.AnyNode;
import flipkart.platform.workflow.node.Nodes;
import flipkart.platform.workflow.node.TypeMismatchException;
import flipkart.platform.workflow.node.junction.AnyJunction;
import flipkart.platform.workflow.node.junction.AnyJunction.Selector;

public class AnyJunctionShowcase
{
    static class Subscriber
    {
        public final String subscriber;

        public Subscriber(String subscriber)
        {
            this.subscriber = subscriber;
        }
    }

    static class Event
    {
        public final String event;

        public Event(String event)
        {
            this.event = event;
        }
    }

    static class Task
    {
        public final Event event;
        public final Subscriber subscriber;

        public Task(Event event, Subscriber subscriber)
        {
            this.event = event;
            this.subscriber = subscriber;
        }
    }

    public static class Enququer extends JobBase<Void> implements
            OneToManyJob<Void, Event>
    {
        @Override
        public Collection<Event> execute(Void i)
                throws ExecutionFailureException
        {
            System.out.println("Enqueuer started...");
            return Arrays
                    .asList(new Event("a"), new Event("b"), new Event("c"));
        }
    }

    public static class Resolver implements Selector<Event>
    {
        @Override
        public void select(AnyNode<?, Event> node, Event i,
                Map<String, AnyNode<?, ?>> fromNodes,
                Map<String, AnyNode<?, ?>> toNodes)
        {
            try
            {
                System.out.println("Selector for event: " + i.event);
                if (i.event.equals("a"))
                {
                    toNodes.get("N2").acceptAny(i);
                }
                else
                {
                    toNodes.get("N3").acceptAny(
                            new Task(i, new Subscriber("S1")));
                }
            }
            catch (TypeMismatchException e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class ArchiverJob extends JobBase<Event> implements
            OneToOneJob<Event, Void>
    {
        @Override
        public Void execute(Event i) throws ExecutionFailureException
        {
            System.out.println("Got event " + i.event);
            return null;
        }
    }

    public static class RecoverJob extends JobBase<Task> implements
            OneToOneJob<Task, Void>
    {
        @Override
        public Void execute(Task i) throws ExecutionFailureException
        {
            System.out.println("Got event " + i.event.event + " for sub: "
                    + i.subscriber.subscriber);
            return null;
        }
    }

    public static void main(String[] args) throws InterruptedException,
            TypeMismatchException
    {
        final AnyNode<Void, Event> n1 = Nodes.newO2MNode("N1", 1, 1,
                Enququer.class).anyNode(Void.class, Event.class);
        final AnyNode<Event, Void> n2 = Nodes.newO2ONode("N2", 1, 1,
                ArchiverJob.class).anyNode(Event.class, Void.class);
        final AnyNode<Task, Void> n3 = Nodes.newO2ONode("N3", 1, 1,
                RecoverJob.class).anyNode(Task.class, Void.class);

        final AnyJunction junction = new AnyJunction();

        junction.from(new Resolver()).bind(n1);
        junction.to(n2);
        junction.to(n3);

        System.out.println("Started");
        n1.acceptAny(null);
        n1.shutdown(true);
        System.out.println("Done");
    }
}
