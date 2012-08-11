package flipkart.platform.hydra.utils;

import java.util.*;
import com.google.common.collect.Maps;

/**
 * User: shashwat
 * Date: 01/08/12
 */
public class UnModifiableMap<K, V> implements Iterable<Map.Entry<K, V>>
{
    private final Map<K, V> wrappedMap;

    @Override
    public Iterator<Map.Entry<K, V>> iterator()
    {
        return wrappedMap.entrySet().iterator();
    }

    public static <K, V> UnModifiableMap<K, V> from(Map<K, V> map)
    {
        return new UnModifiableMap<K, V>(map);
    }

    public static <K, V> UnModifiableMap<K, V> copyOf(Map<K, V> map)
    {
        return new UnModifiableMap<K, V>(Maps.newHashMap(map));
    }

    public UnModifiableMap(Map<K, V> wrappedMap)
    {
        this.wrappedMap = wrappedMap;
    }

    public int size()
    {
        return wrappedMap.size();
    }

    public boolean isEmpty()
    {
        return wrappedMap.isEmpty();
    }

    public boolean containsKey(K key)
    {
        return wrappedMap.containsKey(key);
    }

    public V get(K key)
    {
        return wrappedMap.get(key);
    }

    public Set<K> keySet()
    {
        return Collections.unmodifiableSet(wrappedMap.keySet());
    }


    public Collection<V> values()
    {
        return Collections.unmodifiableCollection(wrappedMap.values());
    }


    public Set<Map.Entry<K, V>> entrySet()
    {
        return Collections.unmodifiableSet(wrappedMap.entrySet());
    }

    @Override
    public String toString()
    {
        return wrappedMap.toString();
    }
}
