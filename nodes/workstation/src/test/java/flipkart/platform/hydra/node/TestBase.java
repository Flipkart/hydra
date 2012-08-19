package flipkart.platform.hydra.node;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.Maps;
import flipkart.platform.hydra.job.AbstractJob;
import flipkart.platform.hydra.job.ExecutionFailureException;
import flipkart.platform.hydra.jobs.ManyToManyJob;
import flipkart.platform.hydra.jobs.OneToManyJob;
import flipkart.platform.hydra.jobs.OneToOneJob;
import flipkart.platform.hydra.topology.SupervisorTopology;
import org.junit.Before;

import static org.junit.Assert.assertFalse;

/**
 * User: shashwat
 * Date: 12/08/12
 */
public class TestBase
{
    protected SupervisorTopology topology;

    public static class TestAbstractJob<I> extends AbstractJob<I>
    {
        @Override
        public void failed(I i, Throwable cause)
        {
            System.out.println("Job failed!! " + i);
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

    @Before
    public void setUp() throws Exception
    {
        topology = new SupervisorTopology();
    }
}
