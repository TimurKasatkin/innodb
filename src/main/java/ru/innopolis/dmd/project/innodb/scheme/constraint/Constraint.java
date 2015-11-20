package ru.innopolis.dmd.project.innodb.scheme.constraint;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;

import java.util.List;
import java.util.function.Predicate;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public interface Constraint extends Predicate<Row> {

    List<Column> getColumns();

}
