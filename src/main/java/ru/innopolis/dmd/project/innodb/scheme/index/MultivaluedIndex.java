package ru.innopolis.dmd.project.innodb.scheme.index;

import ru.innopolis.dmd.project.innodb.scheme.Column;

import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public interface MultivaluedIndex<Key extends Comparable<Key>, Value> {

    void insert(Key key, Value value);

    List<Value> search(Key key);

    List<Column> getColumns();

    void remove(Key key, Value value);

    default void update(Key key, Value oldValue, Value newValue) {
        remove(key, oldValue);
        insert(key, newValue);
    }

}
