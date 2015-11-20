package ru.innopolis.dmd.project.innodb.scheme.constraint;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;

import java.util.Collections;
import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public class NotNull implements Constraint {

    private Column column;

    public NotNull(Column column) {
        this.column = column;
    }

    @Override
    public boolean test(Row row) {
        return row.getValue(column.getName()) != null;
    }

    @Override
    public List<Column> getColumns() {
        return Collections.singletonList(column);
    }
}
