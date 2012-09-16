package flipkart.platform.hydra.link;

import java.util.Queue;
import java.util.UUID;
import com.google.common.collect.Queues;
import flipkart.platform.hydra.job.AbstractJob;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.builder.WSBuilder;
import flipkart.platform.hydra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * User: shashwat
 * Date: 09/08/12
 */
public class JoinOnNodeTest extends JoinTestBase
{
    public static class CheckCasualLeaveJob extends AbstractJob<Person> implements
        OneToOneJob<Person, Pair<Person, Boolean>>
    {
        @Override
        public Pair<Person, Boolean> execute(Person person) throws ExecutionFailureException
        {
            return Pair.of(person, person.casualLeaves > 0);
        }
    }

    public static class CheckPaidLeaveJob extends AbstractJob<Person> implements
        OneToOneJob<Person, Pair<Person, Boolean>>
    {
        @Override
        public Pair<Person, Boolean> execute(Person person) throws ExecutionFailureException
        {
            return Pair.of(person, person.paidLeaves > 0);
        }
    }

    public static class CheckUnPaidLeaveJob extends AbstractJob<Person> implements
        OneToOneJob<Person, Pair<Person, Boolean>>
    {
        @Override
        public Pair<Person, Boolean> execute(Person person) throws ExecutionFailureException
        {
            return Pair.of(person, person.unPaidLeaves > 0);
        }
    }

    public static class PersonJob extends AbstractJob<Integer> implements OneToOneJob<Integer, Person>
    {
        @Override
        public Person execute(Integer index) throws ExecutionFailureException
        {
            final int[] seq = sequences[index];
            return new Person(UUID.randomUUID().toString(), seq[0], seq[1], seq[2]);
        }
    }

    private Node<Integer, Person> personNode;

    private Node<Person, Pair<Person, Boolean>> node1;
    private Node<Person, Pair<Person, Boolean>> node2;
    private Node<Person, Pair<Person, Boolean>> node3;
    private Queue<NodeJoinResult<Person, Boolean>> queue;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        personNode = WSBuilder.withO2OJob(PersonJob.class).build();

        node1 = WSBuilder.withO2OJob(CheckCasualLeaveJob.class).build();
        node2 = WSBuilder.withO2OJob(CheckUnPaidLeaveJob.class).build();
        node3 = WSBuilder.withO2OJob(CheckPaidLeaveJob.class).build();

        queue = Queues.newConcurrentLinkedQueue();
    }

    @After
    public void tearDown() throws Exception
    {
        personNode.shutdown(true);
        queue.clear();
    }

    @Test
    public void test_OR_NodeJoin() throws Exception
    {
        setupJoinLink(new LeaveORJoinPredicate<String>());
        for (int i = 0; i < sequences.length; ++i)
        {
            final NodeJoinResult<Person, Boolean> result = queue.poll();
            assertNotNull("result cannot be null", result);

            if (i == 1)
            {
                assertFalse("Impossible! All type of leaves not available", result.predicateResult);
            }
            else
            {
                assertTrue("Impossible! There are some leaves available", result.predicateResult);
            }
        }
    }

    @Test
    public void test_AND_NodeJoin() throws Exception
    {
        setupJoinLink(new LeaveAndJoinPredicate<String>());
        for (int i = 0; i < sequences.length; ++i)
        {
            final NodeJoinResult<Person, Boolean> result = queue.poll();
            assertNotNull("result cannot be null", result);

            if (i != 0)
            {
                assertFalse("Impossible! Not all leaves not available", result.predicateResult);
            }
            else
            {
                assertTrue("Impossible! All leaves available", result.predicateResult);
            }
        }
    }

    private void setupJoinLink(JoinPredicate<String, Boolean> predicate)
    {
        final NodeJoinLink<Person, Boolean> joinLink = new NodeJoinLink<Person, Boolean>(topology, predicate);
        joinLink.addProducer(personNode);

        joinLink.addFork(node1);
        joinLink.addFork(node2);
        joinLink.addFork(node3);
        joinLink.addConsumer(new ResultNode<NodeJoinResult<Person, Boolean>>("result-node", queue));

        for (int i = 0; i < sequences.length; ++i)
        {
            personNode.accept(i);
        }
    }
}
