package ru.innopolis.dmd.project.innodb.scheme.index;

import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public interface BPlusTreeIndex<Key extends Comparable<Key>, Value> extends Index<Key, Value> {

    List<Value> searchLess(Key key);

    List<Value> searchMore(Key key);

    List<Value> searchBetween(Key key1, Key key2);
}
