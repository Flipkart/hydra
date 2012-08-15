package flipkart.platform.hydra.link;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import flipkart.platform.hydra.job.AbstractJob;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.node.Node;
import flipkart.platform.hydra.node.builder.WSBuilder;
import flipkart.platform.hydra.traits.CanGroup;
import flipkart.platform.hydra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * User: shashwat
 * Date: 09/08/12
 */
public class JoinOnForkTest extends JoinTestBase
{
    public static enum LeaveType
    {
        CASUAL, PAID, LWP //leave without pay
    }

    public static class PersonTask implements CanGroup
    {
        public final Person person;
        public final LeaveType leaveType;

        public PersonTask(Person person, LeaveType leaveType)
        {
            this.person = person;
            this.leaveType = leaveType;
        }

        @Override
        public String getGroupId()
        {
            return person.getGroupId();
        }

        @Override
        public String toString()
        {
            return "PersonTask{" +
                "person=" + person +
                ", leaveType=" + leaveType +
                '}';
        }
    }

    public static class CheckLeaveJob extends AbstractJob<PersonTask> implements
        OneToOneJob<PersonTask, Pair<PersonTask, Boolean>>
    {
        @Override
        public Pair<PersonTask, Boolean> execute(PersonTask personTask) throws ExecutionFailureException
        {
            switch (personTask.leaveType)
            {
            case CASUAL:
                return Pair.of(personTask, personTask.person.casualLeaves > 0);
            case PAID:
                return Pair.of(personTask, personTask.person.paidLeaves > 0);
            case LWP:
                return Pair.of(personTask, personTask.person.unPaidLeaves > 0);
            }
            return null;
        }
    }

    public static class PersonTaskJob extends AbstractJob<Integer> implements OneToOneJob<Integer, List<PersonTask>>
    {
        @Override
        public List<PersonTask> execute(Integer index) throws ExecutionFailureException
        {
            final int[] seq = sequences[index];
            final Person person = new Person(UUID.randomUUID().toString(), seq[0], seq[1], seq[2]);
            final List<PersonTask> list = Lists.newArrayList();
            for (LeaveType leaveType : LeaveType.values())
            {
                list.add(new PersonTask(person, leaveType));
            }
            return list;
        }
    }

    private Node<PersonTask, Pair<PersonTask, Boolean>> forkNode;
    private Node<Integer, List<PersonTask>> personTaskNode;
    private Queue<ForkJoinResult<PersonTask, Boolean>> queue;

    @Before
    public void setUp() throws Exception
    {
        personTaskNode = WSBuilder.withO2OJob(PersonTaskJob.class).build();
        forkNode = WSBuilder.withO2OJob(CheckLeaveJob.class).withName(LeaveType.CASUAL.name()).build();
        queue = Queues.newConcurrentLinkedQueue();
    }

    @After
    public void tearDown() throws Exception
    {
        personTaskNode.shutdown(true);
        queue.clear();
    }

    private void setupJoinLink(JoinPredicate<PersonTask, Boolean> predicate) throws InterruptedException
    {
        final ForkJoinLink<PersonTask, Boolean> joinLink = new ForkJoinLink<PersonTask, Boolean>(predicate);
        joinLink.addSource(personTaskNode);
        joinLink.addFork(forkNode);
        joinLink.addConsumer(new ResultNode<ForkJoinResult<PersonTask, Boolean>>("resultNode", queue));

        for (int i = 0; i < sequences.length; ++i)
        {
            personTaskNode.accept(i);
            Thread.sleep(10);
        }
    }

    @Test
    public void test_OR_ForkJoin() throws Exception
    {
        setupJoinLink(new LeaveORJoinPredicate<PersonTask>());
        for (int i = 0; i < sequences.length; ++i)
        {
            final ForkJoinResult<PersonTask, Boolean> result = queue.poll();
            assertNotNull("result cannot be null", result);

            if (i == 1)
            {
                assertFalse("Impossible! No leaves available", result.predicateResult);
            }
            else
            {
                assertTrue("Impossible! There are some leaves available", result.predicateResult);
            }
        }
    }

    @Test
    public void test_And_ForkJoin() throws Exception
    {
        setupJoinLink(new LeaveAndJoinPredicate<PersonTask>());

        for (int i = 0; i < sequences.length; ++i)
        {
            final ForkJoinResult<PersonTask, Boolean> result = queue.poll();
            assertNotNull("result cannot be null", result);

            if (i != 0)
            {
                assertFalse("Impossible! Not all leaves available: " + result, result.predicateResult);
            }
            else
            {
                assertTrue("Impossible! All leaves available", result.predicateResult);
            }
        }
    }

}
