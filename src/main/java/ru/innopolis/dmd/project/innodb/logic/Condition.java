package ru.innopolis.dmd.project.innodb.logic;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author Timur Kasatkin
 * @date 24.10.15.
 * @email aronwest001@gmail.com
 */
public class Condition implements Predicate<Row> {

    private String columnName;

    private ConditionType conditionType;

    private LogicalOperation logicalOperation;

    private Condition next;

    private List<Comparable> params;

    public Condition(String columnName, ConditionType conditionType, Comparable... params) {
        this(columnName, conditionType, Arrays.asList(params));
    }

    public Condition(String columnName, ConditionType conditionType, List<Comparable> params) {
        this.columnName = columnName;
        this.conditionType = conditionType;
        this.params = params;
    }

    public Condition(String columnName, ConditionType conditionType,
                     List<Comparable> params, LogicalOperation logicalOperation, Condition next) {
        this(columnName, conditionType, params);
        if (logicalOperation == null || next == null)
            throw new IllegalArgumentException();
        this.logicalOperation = logicalOperation;
        this.next = next;
    }

    @Override
    public boolean test(Row row) {
        boolean matches = conditionType.matches(row.getValue(columnName), params);
        if (hasNext())
            matches = logicalOperation.test(matches,
                    next.conditionType.matches(row.getValue(columnName), params));
        return matches;
    }

    public boolean hasNext() {
        return next != null;
    }

    public String getColumnName() {
        return columnName;
    }

    public ConditionType getConditionType() {
        return conditionType;
    }

    public List<Comparable> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return columnName + " " + conditionType + " " + params;
    }
}
