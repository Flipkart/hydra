package flipkart.platform.workflow.utils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * User: shashwat
 * Date: 01/08/12
 */
public class UnModifiableMap<K, V>
{
    private final Map<K, V> wrappedMap;

    public static <K, V> UnModifiableMap<K, V> from(Map<K, V> map)
    {
        return new UnModifiableMap<K, V>(map);
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

    public boolean containsKey(Object key)
    {
        return wrappedMap.containsKey(key);
    }

    public V get(Object key)
    {
        return wrappedMap.get(key);
    }

    public Set<K> keySet()
    {
        return wrappedMap.keySet();
    }


    public Collection<V> values()
    {
        return wrappedMap.values();
    }


    public Set<Map.Entry<K, V>> entrySet()
    {
        return wrappedMap.entrySet();
    }
}
