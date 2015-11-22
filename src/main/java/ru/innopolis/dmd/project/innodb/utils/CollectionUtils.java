package ru.innopolis.dmd.project.innodb.utils;

import ru.innopolis.dmd.project.innodb.Tuple;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class CollectionUtils {

    public static <T> List<T> list(T... vals) {
        return Stream.of(vals).collect(Collectors.toList());
    }

    public static <T> Set<T> set(T... vals) {
        return Stream.of(vals).collect(Collectors.toSet());
    }

    public static <T> Stream<T> stream(Collection<T> collection) {
        return collection.stream();
    }

    public static <T> Stream<T> stream(T... values) {
        return Stream.of(values);
    }

    public static <T> Stream<T> parallelStream(Collection<T> collection) {
        return collection.parallelStream();
    }

    public static <T> Stream<T> parallelStream(T... values) {
        return Stream.of(values).parallel();
    }

    public static Tuple tuple(Comparable... values) {
        return new Tuple(values);
    }

    public static <K, V> Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, V> Map<K, V> map(Entry<K, V>... entries) {
        Map<K, V> map = new LinkedHashMap<>(entries.length);
        for (Entry<K, V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public static <K, V> Map<K, V> map(Collection<Entry<K, V>> entries) {
        Map<K, V> map = new LinkedHashMap<>(entries.size());
        for (Entry<K, V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }



}
