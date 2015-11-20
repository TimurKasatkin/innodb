package ru.innopolis.dmd.project.innodb.scheme.constraint;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class Unique implements Constraint {

    private final List<Column> columns;
    private final Index index;

    public Unique(List<Column> columns, Index index) {
        if (columns == null || columns.size() == 0)
            throw new IllegalArgumentException();
        this.columns = columns;
        this.index = index;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public boolean test(Row row) {
        String concatenation = columns.stream()
                .map(Column::getName)
                .map(row::getValue)
                .map(Object::toString)
                .collect(Collectors.joining());
        return index.search(concatenation) == null;
    }
}
