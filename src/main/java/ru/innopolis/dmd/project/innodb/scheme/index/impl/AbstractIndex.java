package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;

import java.util.List;


/**
 * @author Timur Kasatkin
 * @date 21.11.15.
 * @email aronwest001@gmail.com
 */
public abstract class AbstractIndex<Key extends Comparable<Key>, Value> implements Index<Key, Value> {

    private List<Column> columns;


    protected AbstractIndex(List<Column> columns) {
        this.columns = columns;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }
}
