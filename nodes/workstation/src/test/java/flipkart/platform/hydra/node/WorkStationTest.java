package flipkart.platform.hydra.node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import flipkart.platform.hydra.link.DefaultLink;
import flipkart.platform.hydra.link.GenericLink;
import flipkart.platform.hydra.link.Selector;
import flipkart.platform.hydra.node.builder.WSBuilder;
import flipkart.platform.hydra.utils.HydraThreadFactory;
import flipkart.platform.hydra.utils.Once;
import flipkart.platform.hydra.utils.UnModifiableMap;
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

        splitLineNode = WSBuilder.withO2MJob(SentenceToLines.class).build();
        splitWordNode = WSBuilder.withO2MJob(LinesToWords.class).build();
        freqNode = WSBuilder.withM2MJob(CalculateWordFrequency.class).build();
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
     *     <li>all the nodes are in shutdown state</li>
     *     <li>links have no producers and no consumers</li>
     * </ul>
     * @throws Exception exception
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
        for (Node node : Arrays.<Node> asList(splitLineNode, splitWordNode, freqNode, mergeNode))
        {
            assertTrue("Node (" + node.getIdentity() + ") .isShutdown should return true", node.isShutdown());
        }

        int count = 0;
        for (GenericLink link : Arrays.asList(link1, link2, link3))
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
        final HydraThreadFactory pingThreadFactory = new HydraThreadFactory("Ping");
        final HydraThreadFactory pongThreadFactory = new HydraThreadFactory("Pong");

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
        final HydraThreadFactory pingThreadFactory = new HydraThreadFactory("Ping");
        final HydraThreadFactory pongThreadFactory = new HydraThreadFactory("Pong");

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
        for (int i = 0 ; i < numThreads; ++i)
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
        for (int i = 0 ; i < numThreads; ++i)
        {
            foundThread = foundThread || threads[i].getName().startsWith(namePrefix);
        }
        assertFalse("Thread that begins with: " + namePrefix + " should have been stopped", foundThread);
    }

    private void printWordFrequencyMap()
    {
        for (Map.Entry<String, Integer> entry : mergeNode.mergeFrequencyMap.entrySet())
        {
            System.out.println(entry.getKey() + "->" + entry.getValue());
        }
    }

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

    // TODO: test failure and retry
}
