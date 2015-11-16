package ru.innopolis.dmd.project.innodb.db;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.scheme.Row;
import ru.innopolis.dmd.project.innodb.sql.Query;

import java.util.Collection;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class DataManager {

    public Collection<Row> execute(Query query) {
        StorageManager storageManager = storageManagerBy(query.getTable().getName());
        switch (query.getQueryType()) {
            case SELECT:
                return storageManager.select(query.getPredicate());
            case UPDATE:
                storageManager.update(query.getRow(), query.getPredicate());
                return null;
            case DELETE:
                storageManager.delete(query.getPredicate());
                return null;
            case INSERT:
                storageManager.insert(query.getRow());
                return null;
        }
        return null;
    }

    private StorageManager storageManagerBy(String tableName) {
        checkExistence(tableName);
        return Cache.storageManagers.get(tableName);
    }

    private void checkExistence(String tableName) {
        if (!Cache.tables.containsKey(tableName))
            throw new IllegalArgumentException("There is no such table.");
    }

}
