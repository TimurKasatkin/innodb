package ru.innopolis.dmd.project.innodb.scheme.constraint;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;

import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public class PrimaryKey implements Constraint {

    private final List<Column> columns;
    private final PKIndex index;

    public PrimaryKey(PKIndex index) {
        if (index == null || index.getColumns() == null || index.getColumns().size() == 0)
            throw new IllegalArgumentException();
        this.columns = index.getColumns();
        this.index = index;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public boolean test(Row row) {
        String valuesConcatenation = "";
        for (Column column : columns) {
            Comparable value = row.v(column.getName());
            if (value == null) return false;
            valuesConcatenation += value;
        }
        return index.search(valuesConcatenation) == null;
    }
}
