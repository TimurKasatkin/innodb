package ru.innopolis.dmd.project.innodb.db.index;

import java.util.List;
import java.util.Map;

/**
 * @author Timur Kasatkin
 * @date 25.10.15.
 * @email aronwest001@gmail.com
 */
public interface BPlusTree<Key extends Comparable, Value> {

    void insert(Key key, Value value);

    List<Value> search(Key key);

    List<Value> searchLess(Key key);

    List<Value> searchMore(Key key);

    List<Value> searchBetween(Key key1, Key key2);

    void remove(Key key, Value value);

    List<Map.Entry<Key, List<Value>>> neighbours(Key key, int numOfNeighbours);

}
