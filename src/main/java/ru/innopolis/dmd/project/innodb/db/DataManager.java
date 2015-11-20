package ru.innopolis.dmd.project.innodb.db;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.sql.RowPredicate;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class DataManager {

    public void insert(Row row, String tableName) {
        checkTableExists(tableName);
        Table table = Cache.getTable(tableName);
        insert(row, table);
    }

    public void insert(Row row, Table table) {
        table.test(row);
    }

    public void delete(RowPredicate rowPredicate, String tableName) {
        checkTableExists(tableName);
        Table table = Cache.getTable(tableName);
        delete(rowPredicate, table);
    }

    public void delete(RowPredicate rowPredicate, Table table) {

    }

    private void checkTableExists(String tableName) {
        if (!Cache.tables.containsKey(tableName))
            throw new IllegalArgumentException("There is no such table.");
    }
}
