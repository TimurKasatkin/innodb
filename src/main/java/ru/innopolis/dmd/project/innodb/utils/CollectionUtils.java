package ru.innopolis.dmd.project.innodb.utils;

import java.util.Collection;
import java.util.List;
import java.util.Set;
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

    public static <T> Stream<T> parallelStream(Collection<T> collection) {
        return collection.parallelStream();
    }

}
