package ru.innopolis.dmd.project.innodb.sql;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.logic.Condition;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Timur Kasatkin
 * @date 25.10.15.
 * @email aronwest001@gmail.com
 */
public class RowPredicate implements Predicate<Row> {

    private Predicate<Row> predicate;

    private List<Condition> conditions;

    private List<String> columnNames;

    public RowPredicate(Predicate<Row> predicate, Condition... conditions) {
        this(predicate, Arrays.asList(conditions));
//        new RowPredicate(row -> row.v("name").compareTo())
//        new Condition("name", LESS_OR_EQUALS, list(5),AND,new Condition())
    }

    public RowPredicate(Predicate<Row> predicate, List<Condition> conditions) {
        this.conditions = conditions == null ? new LinkedList<>() : conditions;
        this.predicate = predicate;
        this.columnNames = conditions == null ?
                new LinkedList<>() :
                conditions.stream().map(Condition::getColumnName).collect(Collectors.toList());
    }

    @Override
    public boolean test(Row row) {
        return predicate.test(row);
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }
}
