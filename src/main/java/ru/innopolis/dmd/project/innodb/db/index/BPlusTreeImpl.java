package ru.innopolis.dmd.project.innodb.db.index;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author Timur Kasatkin
 * @date 26.10.15.
 * @email aronwest001@gmail.com
 */
public class BPlusTreeImpl<Key extends Comparable, Value> implements BPlusTree<Key, Value> {

    private File file;
    private DB db;
    private BTreeMap<Key, List<Value>> treeMap;

    public BPlusTreeImpl(File file) {
        this.file = file;
        db = DBMaker.newFileDB(file).make();
        treeMap = db.getTreeMap(file.getName());
    }

    public static void main(String[] args) {
        BPlusTree<String, Long> tree = new BPlusTreeImpl<>(
                new File(DBConstants.INDEXES_FILES_DIRECTORY + "Employee_index_name.txt"));
        List<Entry<String, List<Long>>> neighbours = tree.neighbours("name 10", 2);
        System.out.println(neighbours.size() + " neighbours actually: " + neighbours);
    }

    @Override
    public void insert(Key key, Value value) {
        List<Value> values = treeMap.get(key);
        if (values == null) {
            values = new LinkedList<>();
            treeMap.put(key, values);
        }
        values.add(value);
        db.commit();
    }

    @Override
    public List<Value> search(Key key) {
        return treeMap.get(key);
    }

    @Override
    public List<Value> searchLess(Key key) {
        Entry<Key, List<Value>> lowerEntry = treeMap.lowerEntry(key);
        List<Value> values = new LinkedList<>();
        while (lowerEntry != null) {
            values.addAll(lowerEntry.getValue());
            lowerEntry = treeMap.lowerEntry(lowerEntry.getKey());
        }
        return values;
    }

    @Override
    public List<Value> searchMore(Key key) {
        Entry<Key, List<Value>> higherEntry = treeMap.higherEntry(key);
        List<Value> values = new LinkedList<>();
        while (higherEntry != null) {
            values.addAll(higherEntry.getValue());
            higherEntry = treeMap.higherEntry(higherEntry.getKey());
        }
        return values;
    }

    @Override
    public List<Value> searchBetween(Key key1, Key key2) {
        throw new NotImplementedException();
    }

    @Override
    public void remove(Key key, Value value) {
        List<Value> values = treeMap.get(key);
        if (values != null) {
            values.remove(value);
            if (values.isEmpty()) treeMap.remove(key);
            db.commit();
        }
    }

    @Override
    public List<Entry<Key, List<Value>>> neighbours(Key key, int numOfNeighbours) {
        List<Entry<Key, List<Value>>> neighbours = new LinkedList<>();
        Key curLowerKey = key;
        Key curHigherKey = key;
        int count = 0;
        Entry<Key, List<Value>> lowerKey = null;
        Entry<Key, List<Value>> higherEntry = null;
        while (count < numOfNeighbours && !(curLowerKey == null && curHigherKey == null)) {
            if (curLowerKey != null) {
                lowerKey = treeMap.lowerEntry(curLowerKey);
            }
            if (curHigherKey != null) {
                higherEntry = treeMap.higherEntry(curHigherKey);
            }
            if (lowerKey != null) {
                neighbours.add(lowerKey);
                count++;
            }
            curLowerKey = lowerKey == null ? null : lowerKey.getKey();
            if (count >= numOfNeighbours) break;
            if (higherEntry != null) {
                neighbours.add(higherEntry);
                count++;
            }
            curHigherKey = higherEntry == null ? null : higherEntry.getKey();
        }
        Collections.sort(neighbours, (o1, o2) -> o1.getKey().compareTo(o2.getKey()));
        return neighbours;
    }

}
