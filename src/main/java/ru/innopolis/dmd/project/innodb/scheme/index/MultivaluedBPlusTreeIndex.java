package ru.innopolis.dmd.project.innodb.scheme.index;

import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 25.10.15.
 * @email aronwest001@gmail.com
 */
public interface MultivaluedBPlusTreeIndex<Key extends Comparable<Key>, Value>
        extends MultivaluedIndex<Key, Value> {

    List<Value> searchLess(Key key);

    List<Value> searchMore(Key key);

    List<Value> searchBetween(Key key1, Key key2);

}
