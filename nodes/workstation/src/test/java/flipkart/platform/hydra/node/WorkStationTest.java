package flipkart.platform.hydra.node;

import java.util.*;
import java.util.concurrent.*;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.link.DefaultLink;
import flipkart.platform.hydra.link.Link;
import flipkart.platform.hydra.link.Selector;
import flipkart.platform.hydra.node.builder.WSBuilder;
import flipkart.platform.hydra.node.workstation.BasicWorkStation;
import flipkart.platform.hydra.queue.ConcurrentQueue;
import flipkart.platform.hydra.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static flipkart.platform.hydra.link.LinkBuilder.link;
import static flipkart.platform.hydra.link.LinkBuilder.using;
import static org.junit.Assert.*;

/**
 * User: shashwat
 * Date: 12/08/12
 */
public class WorkStationTest extends TestBase
{
    private static final String RANDOM_PARA =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin placerat, tortor non varius elementum, " +
            "mi erat adipiscing felis, sed sollicitudin neque augue id orci. Integer egestas tempus feugiat. Nullam " +
            "semper ornare varius. Aenean facilisis sagittis urna a viverra. In tempor odio eget felis scelerisque " +
            "mollis. Phasellus eu posuere lacus. Vivamus lorem tortor, pharetra ut malesuada a, " +
            "fringilla vitae arcu. Donec nec mollis neque.Suspendisse iaculis ultricies semper. Fusce in magna eget " +
            "magna sagittis tristique nec sit amet lacus. Integer sed est ligula. Sed et tincidunt elit. Donec " +
            "commodo aliquam nibh in pellentesque. Fusce feugiat tempor metus sed tempus.";

    private Node<String, String> splitLineNode;
    private Node<String, String> splitWordNode;
    private Node<String, Map<String, Integer>> freqNode;
    private Node<String, String> wordSanitizer;
    private Node<String, String> toUpper;
    private MergeWordFrequencies mergeNode;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();

        splitLineNode = WSBuilder.withO2MJob(SentenceToLines.class).withThreadExecutor(2).build();
        splitWordNode = WSBuilder.withO2MJob(LinesToWords.class).build();
        freqNode = WSBuilder.withM2MJob(CalculateWordFrequency.class).withBatch(5, 5).build();
        wordSanitizer = WSBuilder.withO2OJob(WordSanitizer.class).build();

        toUpper = WSBuilder.withO2OJob(ToUpperCase.class).build();

        mergeNode = new MergeWordFrequencies("merger");
    }

    @After
    public void tearDown() throws Exception
    {
        //Thread.sleep(100000);
    }

    @Test
    public void testSimpleTopology() throws Exception
    {
        assertTrue("Map must be empty", mergeNode.mergeFrequencyMap.isEmpty());

        link(topology, splitLineNode).to(splitWordNode).to(freqNode).to(mergeNode);

        splitLineNode.accept(RANDOM_PARA);

        topology.shutdown(true);

        assertFalse("Map cannot be empty", mergeNode.mergeFrequencyMap.isEmpty());
        assertEquals("comma [,] occurs 6 times", 6, (long) mergeNode.mergeFrequencyMap.get(","));
        assertEquals("'semper' occurs 2 times", 2, (long) mergeNode.mergeFrequencyMap.get("semper"));
        assertEquals("'viverra' occurs 1 times", 1, (long) mergeNode.mergeFrequencyMap.get("viverra"));
    }

    /**
     * Some shutdown guarantees at the end of single topology shutdown
     * <ul>
     * <li>all the nodes are in shutdown state</li>
     * <li>links have no producers and no consumers</li>
     * </ul>
     *
     * @throws Exception
     *     exception
     */
    @Test
    public void testShutdownGuarantees() throws Exception
    {
        assertTrue("Map must be empty", mergeNode.mergeFrequencyMap.isEmpty());
        final DefaultLink<String> link1 = new DefaultLink<String>(topology);
        link1.addProducer(splitLineNode);
        link1.addConsumer(splitWordNode);

        final DefaultLink<String> link2 = new DefaultLink<String>(topology);
        link2.addProducer(splitWordNode);
        link2.addConsumer(freqNode);

        final DefaultLink<Map<String, Integer>> link3 = new DefaultLink(topology);
        link3.addProducer(freqNode);
        link3.addConsumer(mergeNode);

        splitLineNode.accept(RANDOM_PARA);

        topology.shutdown(true);

        assertTrue("topology.isShutdown should return true", topology.isShutdown());
        for (Node node : Arrays.<Node>asList(splitLineNode, splitWordNode, freqNode, mergeNode))
        {
            assertTrue("Node (" + node.getIdentity() + ") .isShutdown should return true", node.isShutdown());
        }

        int count = 0;
        for (Link link : Arrays.asList(link1, link2, link3))
        {
            final int i = ++count;
            assertTrue("Link" + i + " must not have any producers", link.getProducers().isEmpty());
            assertTrue("Link" + i + " must not have any consumers", link.getProducers().isEmpty());
        }
    }

    @Test
    public void testNodeReturnsNull() throws Exception
    {
        assertTrue("Map must be empty", mergeNode.mergeFrequencyMap.isEmpty());

        // wordSanitizer returns null if the input string do not consist of only alphabets
        link(topology, splitLineNode).to(splitWordNode).to(wordSanitizer).to(freqNode).to(mergeNode);

        splitLineNode.accept(RANDOM_PARA);
        topology.shutdown(true);

        assertFalse("Map cannot be empty", mergeNode.mergeFrequencyMap.isEmpty());
        assertNull("There is no comma [,]", mergeNode.mergeFrequencyMap.get(","));
        assertEquals("'semper' occurs 2 times", 2, (long) mergeNode.mergeFrequencyMap.get("semper"));
        assertEquals("'viverra' occurs 1 times", 1, (long) mergeNode.mergeFrequencyMap.get("viverra"));
    }

    @Test
    public void testNodeSelectorLink() throws Exception
    {
        assertTrue("Map must be empty", mergeNode.mergeFrequencyMap.isEmpty());

        // wordSanitizer returns null if the input string does not consists of only alphabets
        link(topology, splitLineNode).to(splitWordNode).toOnly(wordSanitizer);
        using(topology, new VowelSelector()).linkFrom(wordSanitizer).to(toUpper, freqNode);
        link(topology, toUpper).to(freqNode).toOnly(mergeNode);

        splitLineNode.accept(RANDOM_PARA);
        topology.shutdown(true);

        assertFalse("Map cannot be empty", mergeNode.mergeFrequencyMap.isEmpty());
        assertNull("There is no comma [,]", mergeNode.mergeFrequencyMap.get(","));
        assertEquals("'semper' occurs 2 times", 2, (long) mergeNode.mergeFrequencyMap.get("semper"));
        assertEquals("'viverra' occurs 1 times", 1, (long) mergeNode.mergeFrequencyMap.get("viverra"));
        assertEquals("'AMET' occurs 2 times", 2, (long) mergeNode.mergeFrequencyMap.get("AMET"));
        assertNull("'a' occurs 0 times", mergeNode.mergeFrequencyMap.get("a"));
    }

    @Test
    public void testCircularDependency() throws Exception
    {
        final DefaultThreadFactory pingThreadFactory = new DefaultThreadFactory("Ping");
        final DefaultThreadFactory pongThreadFactory = new DefaultThreadFactory("Pong");

        final Node<String, String> pingNode =
            WSBuilder.withO2OJob(Ping.class).withExecutor(Executors.newFixedThreadPool(1, pingThreadFactory)).build();
        final Node<String, String> pongNode = WSBuilder.withO2OJob(Pong.class).withExecutor(
            Executors.newFixedThreadPool(1, pongThreadFactory)).build();

        link(topology, pingNode).to(pongNode).toOnly(pingNode);

        pingNode.accept("pong");
        Thread.sleep(200);
        assertThreadRunningWithName(pingThreadFactory.getThreadGroup(), 5);
        assertThreadRunningWithName(pongThreadFactory.getThreadGroup(), 5);

        final Once<Boolean> once = new Once<Boolean>(false);
        final Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                topology.shutdown(true);
                once.set(topology.isShutdown());
            }
        };
        thread.start();
        thread.join(200);
        once.set(false);

        assertTrue("Terminated after some time", once.get());
        assertTrue("Ping node terminated", pingNode.isShutdown());
        assertTrue("Pong node terminated", pongNode.isShutdown());

        assertThreadStoppedWithName(pingThreadFactory.getThreadGroup(), 5);
        assertThreadStoppedWithName(pongThreadFactory.getThreadGroup(), 5);
    }

    @Test
    public void testShutdownNoWaitForTermination() throws Exception
    {
        final DefaultThreadFactory pingThreadFactory = new DefaultThreadFactory("Ping");
        final DefaultThreadFactory pongThreadFactory = new DefaultThreadFactory("Pong");

        final Node<String, String> pingNode =
            WSBuilder.withO2OJob(Ping.class).withExecutor(Executors.newFixedThreadPool(1, pingThreadFactory)).build();
        final Node<String, String> pongNode = WSBuilder.withO2OJob(Pong.class).withExecutor(
            Executors.newFixedThreadPool(1, pongThreadFactory)).build();

        link(topology, pingNode).to(pongNode).toOnly(pingNode);

        pingNode.accept("pong");

        Thread.sleep(200);
        assertThreadRunningWithName(pingThreadFactory.getThreadGroup(), 5);
        assertThreadRunningWithName(pongThreadFactory.getThreadGroup(), 5);

        final Once<Boolean> isShutdown = new Once<Boolean>(false);
        final Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                topology.shutdown(false);
                isShutdown.set(topology.isShutdown());
            }
        };
        thread.start();
        thread.join();
        Thread.sleep(500);
        assertTrue("Terminated after some time", isShutdown.isSet() && isShutdown.get());
        assertTrue("Ping node should terminated", pingNode.isShutdown());
        assertTrue("Pong node terminated", pongNode.isShutdown());

        assertThreadStoppedWithName(pingThreadFactory.getThreadGroup(), 5);
        assertThreadStoppedWithName(pongThreadFactory.getThreadGroup(), 5);
    }

    private void assertThreadRunningWithName(ThreadGroup threadGroup, int maxThreads)
    {
        final Thread[] threads = new Thread[maxThreads];
        final String namePrefix = threadGroup.getName();

        final int numThreads = threadGroup.enumerate(threads);
        assertTrue("There should be more than 1 thread in " + namePrefix + " thread group", numThreads > 0);
        boolean foundThread = false;
        for (int i = 0; i < numThreads; ++i)
        {
            foundThread = foundThread || threads[i].getName().startsWith(namePrefix);
        }
        assertTrue("There should be a thread that begins with: " + namePrefix, foundThread);
    }

    private void assertThreadStoppedWithName(ThreadGroup threadGroup, int maxThreads)
    {
        final Thread[] threads = new Thread[maxThreads];
        final String namePrefix = threadGroup.getName();

        final int numThreads = threadGroup.enumerate(threads);
        assertTrue("There should be >=0 thread in " + namePrefix + " thread group", 0 <= numThreads);
        boolean foundThread = false;
        for (int i = 0; i < numThreads; ++i)
        {
            foundThread = foundThread || threads[i].getName().startsWith(namePrefix);
        }
        assertFalse("Thread that begins with \'" + namePrefix + "\' should have been stopped", foundThread);
    }

    //private void printWordFrequencyMap()
    //{
    //    for (Map.Entry<String, Integer> entry : mergeNode.mergeFrequencyMap.entrySet())
    //    {
    //        System.out.println(entry.getKey() + "->" + entry.getValue());
    //    }
    //}

    private static class VowelSelector implements Selector<String>
    {
        @Override
        public Collection<Node<String, ?>> select(String i, UnModifiableMap<String, Node<String, ?>> nodes)
        {
            final ArrayList<Node<String, ?>> list = new ArrayList<Node<String, ?>>(1);
            if (i.matches("^[aeiouAEIOU].*"))
            {
                list.add(nodes.get("ToUpperCase"));
            }
            else
            {
                list.add(nodes.get("CalculateWordFrequency"));
            }
            return list;
        }
    }

    private static class TestJobFactoryWrapper<J> implements JobFactory<J>
    {
        private final List<J> list = Lists.newLinkedList();
        private final JobFactory<J> jobFactory;

        public TestJobFactoryWrapper(JobFactory<J> jobFactory)
        {
            this.jobFactory = jobFactory;
        }

        @Override
        public J newJob()
        {
            final J j = jobFactory.newJob();
            list.add(j);
            return j;
        }

        public UnModifiableCollection<J> getJobList()
        {
            return UnModifiableCollection.from(list);
        }
    }

    @Test
    public void testInitializableJobInThreadExecutorNode() throws Exception
    {
        final int numThreads = 2;
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final TestJobFactoryWrapper<InitializableJob> testJobFactory =
            new TestJobFactoryWrapper(new InitializableJob.Factory(latch));

        final Node<String, String> node = WSBuilder.withO2OJobFactory("InitializableJob", testJobFactory)
            .withThreadExecutor(numThreads).build();

        for (int i = 0; i < 10; ++i)
        {
            node.accept("something-" + i);
        }
        node.shutdown(true);
        latch.await();

        final UnModifiableCollection<InitializableJob> jobList = testJobFactory.getJobList();
        assertTrue("Job instances count should not be greater equal to number of threads",
            numThreads >= jobList.size());

        for (InitializableJob job : jobList)
        {
            assertNotNull("job instance cannot be null", job);
            assertEquals("Init counter should be 1.", 1, job.initCounter.get());
            assertEquals("Destroy counter should be 1", 1, job.destroyCounter.get());
            assertTrue("init() must be called before destroy()",
                job.initBeforeDestroy.isSet() && job.initBeforeDestroy.get());
            assertTrue("init() must be called before execute()",
                job.initBeforeExecute.isSet() && job.initBeforeExecute.get());
        }
    }

    @Test
    public void testNoRetry() throws Exception
    {
        final int maxRetries = 3;
        final int maxFailures = maxRetries - 1;

        final RetryPolicy<String> retryPolicy = new NoRetryPolicy<String>();
        final TestJobFactoryWrapper<AlwaysFailingTestJob> testJobFactoryWrapper =
            new TestJobFactoryWrapper(new AlwaysFailingTestJob.Factory(maxFailures));

        executeInitializableJobTestNode(retryPolicy, testJobFactoryWrapper);

        final UnModifiableCollection<AlwaysFailingTestJob> jobList = testJobFactoryWrapper.getJobList();
        for (AlwaysFailingTestJob job : jobList)
        {
            assertEquals("Job execution count should be 1", 1, job.executionCounter.get());
            assertTrue("Job is marked as failed and failed() is called after execute()",
                job.failedCalledAfterExecute.isSet() && job.failedCalledAfterExecute.get());
        }
    }

    @Test
    public void testRetryAndRecover() throws Exception
    {
        final int maxRetries = 4;
        final int maxFailures = maxRetries - 2;

        final RetryPolicy<String> retryPolicy = new DefaultRetryPolicy<String>(maxRetries);

        final TestJobFactoryWrapper<AlwaysFailingTestJob> testJobFactoryWrapper =
            new TestJobFactoryWrapper(new AlwaysFailingTestJob.Factory(maxFailures));

        executeInitializableJobTestNode(retryPolicy, testJobFactoryWrapper);

        final UnModifiableCollection<AlwaysFailingTestJob> jobList = testJobFactoryWrapper.getJobList();
        for (AlwaysFailingTestJob job : jobList)
        {
            assertEquals("Execution count should be 3. Check if the job re-executed.", maxFailures + 1,
                job.executionCounter.get());
            assertFalse("Job should succeed", job.failedCalledAfterExecute.isSet());
        }
    }

    @Test
    public void testJobFailureAfterRetry() throws Exception
    {
        final int maxRetries = 4;

        final RetryPolicy<String> retryPolicy = new DefaultRetryPolicy<String>(maxRetries);

        final TestJobFactoryWrapper<AlwaysFailingTestJob> testJobFactoryWrapper =
            new TestJobFactoryWrapper(new AlwaysFailingTestJob.Factory(maxRetries));

        executeInitializableJobTestNode(retryPolicy, testJobFactoryWrapper);

        final UnModifiableCollection<AlwaysFailingTestJob> jobList = testJobFactoryWrapper.getJobList();
        for (AlwaysFailingTestJob job : jobList)
        {
            assertEquals("Execution count should be 3. Check if the job re-executed.", maxRetries,
                job.executionCounter.get());
            assertTrue("Job should fail", job.failedCalledAfterExecute.isSet() && job.failedCalledAfterExecute.get());
        }
    }

    private void executeInitializableJobTestNode(RetryPolicy<String> retryPolicy,
        TestJobFactoryWrapper<AlwaysFailingTestJob> testJobFactoryWrapper) throws
        NoSuchMethodException, InterruptedException
    {
        final Node<String, String> node =
            WSBuilder.withO2OJobFactory("AlwaysFailingTestJob", testJobFactoryWrapper).withRetry(
                retryPolicy).build();

        node.accept("something");
        node.shutdown(true);
    }

    @Test
    public void testBasicJob() throws Exception
    {
        final Node<String, String> basic =
            new BasicWorkStation("basic", MoreExecutors.sameThreadExecutor(), ConcurrentQueue.newQueue(),
                DefaultJobFactory.create(BasicJobImpl.class));
        basic.accept("world");
        basic.shutdown(true);
    }

    private static class AlwaysThrowRejectedExecutionExceptionExecutor extends AbstractExecutorService
    {
        private final RunState runState = new RunState();

        @Override
        public void shutdown()
        {
            runState.shutdownNow();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            runState.shutdownNow();
            return null;
        }

        @Override
        public boolean isShutdown()
        {
            return !runState.isActive();
        }

        @Override
        public boolean isTerminated()
        {
            return runState.isShutdown();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return isShutdown();
        }

        @Override
        public void execute(Runnable command)
        {
            throw new RejectedExecutionException("I do not accept commands!");
        }
    }

    @Test
    public void testExecutorThrowsRejectedExecutionException() throws Exception
    {
        final ConcurrentQueue<String> queue = ConcurrentQueue.newQueue();
        final Node<String, String> node = WSBuilder.withO2OJob(WordSanitizer.class).withExecutor(
            new AlwaysThrowRejectedExecutionExceptionExecutor()).withQueue(queue).build();

        assertTrue("Queue is initially empty", queue.isEmpty());
        node.accept("World!");
        assertTrue("Queue should be finally empty", queue.isEmpty());
        node.shutdown(true);
    }
}
