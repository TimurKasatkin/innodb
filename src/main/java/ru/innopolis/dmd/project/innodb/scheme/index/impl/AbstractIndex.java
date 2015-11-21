package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;

import java.util.List;


/**
 * @author Timur Kasatkin
 * @date 21.11.15.
 * @email aronwest001@gmail.com
 */
public abstract class AbstractIndex<Key extends Comparable<Key>, Value> implements Index<Key, Value> {

    protected Table table;
    private List<Column> columns;
    private String tableName;

    protected AbstractIndex(String tableName, List<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public Table getTable() {
        if (table == null) {
            synchronized (this) {
                if (table == null) {
                    table = Cache.getTable(tableName);
                }
            }
        }
        return table;
    }
}
