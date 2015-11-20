package ru.innopolis.dmd.project.innodb.scheme.index;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public interface Index<Key extends Comparable<Key>, Value> {

    Value search(Key key);

    void insert(Key key, Value value);

    void remove(Key key);

    default void update(Key key, Value value) {
        remove(key);
        insert(key, value);
    }

}
