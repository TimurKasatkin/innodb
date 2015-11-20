package ru.innopolis.dmd.project.innodb.scheme.constraint;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;

import java.util.List;
import java.util.stream.Collectors;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.stream;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public class ForeignKey implements Constraint {

    private final PKIndex index;
    private final List<Column> columns;

    public ForeignKey(List<Column> columns, PKIndex index) {
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
        String concatenation = stream(columns)
                .map(Column::getName)
                .map(row::getValue)
                .map(Object::toString)
                .collect(Collectors.joining());
        return index.search(concatenation) != null;
    }

}
