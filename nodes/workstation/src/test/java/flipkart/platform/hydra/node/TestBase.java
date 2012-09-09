package flipkart.platform.hydra.node;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.common.JobExecutionContext;
import flipkart.platform.hydra.common.MessageCtx;
import flipkart.platform.hydra.job.AbstractJob;
import flipkart.platform.hydra.job.BasicJob;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.job.JobFactory;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.jobs.OneToManyJob;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.topology.LinkTopology;
import flipkart.platform.hydra.traits.Initializable;
import flipkart.platform.hydra.utils.Once;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.junit.Assert.assertFalse;

/**
 * User: shashwat
 * Date: 12/08/12
 */
public class TestBase
{
    protected LinkTopology topology;

    public static class TestAbstractJob<I> extends AbstractJob<I>
    {
        @Override
        public void failed(I i, Throwable cause)
        {
            super.failed(i, cause);
            System.out.println("Job failed!! " + i);
            cause.printStackTrace();
            assertFalse("No jobs can fail!!!!", true);
        }
    }

    public static class SentenceToLines extends TestAbstractJob<String> implements OneToManyJob<String, String>
    {
        @Override
        public Collection<String> execute(String s) throws ExecutionFailureException
        {
            final String[] lines = s.split("[.\\n]");
            List<String> list = new ArrayList<String>(lines.length);
            for (String aLine : lines)
            {
                list.add(aLine.trim());
            }
            return list;
        }
    }

    public static class LinesToWords extends TestAbstractJob<String> implements OneToManyJob<String, String>
    {
        @Override
        public Collection<String> execute(String s) throws ExecutionFailureException
        {
            final String[] words = s.split("\\b");
            List<String> list = new ArrayList<String>(words.length);
            for (String aWord : words)
            {
                final String trimmedWord = aWord.trim();
                if (!trimmedWord.isEmpty())
                {
                    list.add(trimmedWord);
                }
            }
            return list;
        }
    }

    public static class CalculateWordFrequency extends TestAbstractJob<String> implements
        ManyToManyJob<String, Map<String, Integer>>
    {
        @Override
        public Collection<Map<String, Integer>> execute(List<String> words) throws ExecutionFailureException
        {
            final Map<String, Integer> wordFrequencyMap = Maps.newHashMap();
            for (String word : words)
            {
                final Integer integer = wordFrequencyMap.get(word);
                if (integer == null)
                {
                    wordFrequencyMap.put(word, 1);
                }
                else
                {
                    wordFrequencyMap.put(word, integer + 1);
                }
            }

            return Arrays.asList(wordFrequencyMap);

        }
    }

    public static class WordSanitizer extends TestAbstractJob<String> implements OneToOneJob<String, String>
    {
        @Override
        public String execute(String s) throws ExecutionFailureException
        {
            if (s.matches("[a-zA-Z]+"))
            {
                return s.toLowerCase();
            }
            return null;
        }
    }

    public static class ToUpperCase extends TestAbstractJob<String> implements OneToOneJob<String, String>
    {
        @Override
        public String execute(String s) throws ExecutionFailureException
        {
            return s.toUpperCase();
        }
    }

    public static class MergeWordFrequencies extends AbstractNodeBase<Map<String, Integer>, Void>
    {
        public final ConcurrentMap<String, Integer> mergeFrequencyMap;

        protected MergeWordFrequencies(String name)
        {
            super(name);
            this.mergeFrequencyMap = Maps.newConcurrentMap();
        }

        @Override
        protected void shutdownResources(boolean awaitTermination) throws InterruptedException
        {
        }

        @Override
        protected void acceptMessage(Map<String, Integer> map)
        {
            for (Map.Entry<String, Integer> entry : map.entrySet())
            {
                final Integer integer = mergeFrequencyMap.putIfAbsent(entry.getKey(), entry.getValue());
                if (integer != null)
                {
                    synchronized (this)
                    {
                        final Integer val = mergeFrequencyMap.get(entry.getKey());
                        mergeFrequencyMap.put(entry.getKey(), val + entry.getValue());
                    }
                }
            }
        }
    }

    public static class Ping implements OneToOneJob<String, String>
    {
        @Override
        public String execute(String s) throws ExecutionFailureException
        {
            return s.equals("pong") ? "ping" : null;
        }

        @Override
        public void failed(String s, Throwable cause)
        {
            // Test will fail while shutting down, so no asserts
        }
    }

    public static class Pong implements OneToOneJob<String, String>
    {
        @Override
        public String execute(String s) throws ExecutionFailureException
        {
            return s.equals("ping") ? "pong" : null;
        }

        @Override
        public void failed(String s, Throwable cause)
        {
            // Test will fail while shutting down, so no asserts
        }

    }

    public static class InitializableJob extends TestAbstractJob<String> implements OneToOneJob<String, String>,
        Initializable
    {
        public static class Factory implements JobFactory<InitializableJob>
        {
            private final CountDownLatch destroyLatch;

            public Factory(CountDownLatch destroyLatch)
            {
                this.destroyLatch = destroyLatch;
            }

            @Override
            public InitializableJob newJob()
            {
                return new InitializableJob(destroyLatch);
            }
        }

        private final CountDownLatch destroyLatch;
        public final AtomicInteger initCounter = new AtomicInteger(0);
        public final AtomicInteger destroyCounter = new AtomicInteger(0);
        public final Once<Boolean> initBeforeExecute = new Once<Boolean>();
        public final Once<Boolean> initBeforeDestroy = new Once<Boolean>();

        public InitializableJob(CountDownLatch destroyLatch)
        {
            this.destroyLatch = destroyLatch;
        }

        @Override
        public String execute(String s) throws ExecutionFailureException
        {
            initBeforeExecute.set(initCounter.get() != 0);
            return s;
        }

        @Override
        public void init()
        {
            initCounter.incrementAndGet();
        }

        @Override
        public void destroy()
        {
            try
            {
                initBeforeDestroy.set(initCounter.get() != 0);
                destroyCounter.incrementAndGet();
                destroyLatch.countDown();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test failures
     */
    public static class AlwaysFailingTestJob implements OneToOneJob<String, String>
    {
        public static class Factory implements JobFactory<AlwaysFailingTestJob>
        {
            private final int maxNumFailures;

            public Factory(int maxNumFailures)
            {
                this.maxNumFailures = maxNumFailures;
            }

            @Override
            public AlwaysFailingTestJob newJob()
            {
                return new AlwaysFailingTestJob(maxNumFailures);
            }
        }

        public final AtomicInteger executionCounter = new AtomicInteger(0);
        public final Once<Boolean> failedCalledAfterExecute = new Once<Boolean>();

        private final int maxNumFailures;

        public AlwaysFailingTestJob(int maxNumFailures)
        {
            this.maxNumFailures = maxNumFailures;
        }

        @Override
        public String execute(String s) throws ExecutionFailureException
        {
            final int i = executionCounter.incrementAndGet();
            if (i <= maxNumFailures)
            {
                throw new ExecutionFailureException(s);
            }
            return s;
        }

        @Override
        public void failed(String s, Throwable cause)
        {
            failedCalledAfterExecute.set(executionCounter.get() != 0);
        }
    }

    public static class BasicJobImpl extends TestAbstractJob<String> implements BasicJob<String, String>
    {
        @Override
        public void execute(MessageCtx<String> i,
            JobExecutionContext<String, String, BasicJob<String, String>> executionContext)
        {
            executionContext.succeeded(i);
            executionContext.submitResponse("Hello! " + i.get());
            executionContext.end();
        }
    }

    @Before
    public void setUp() throws Exception
    {
        topology = new LinkTopology();
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        //ConsoleReporter.enable(10, TimeUnit.MILLISECONDS);
    }
}
