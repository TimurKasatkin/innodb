package ru.innopolis.dmd.project.innodb.logic;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.list;

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
        this(columnName, conditionType, list(params));
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
            matches = logicalOperation.test(matches, next.test(row));
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

    public List<String> getColumnNames() {
        List<String> columnNames = new LinkedList<>();
        columnNames.add(this.columnName);
        if (hasNext())
            columnNames.addAll(next.getColumnNames());
        return columnNames;
    }

    public List<Condition> asList() {
        List<Condition> conditions = new LinkedList<>();
        conditions.add(this);
        if (hasNext())
            conditions.addAll(next.asList());
        return conditions;
    }

    public List<ConditionType> conditionTypes() {
        List<ConditionType> conditionTypes = new LinkedList<>();
        conditionTypes.add(this.conditionType);
        if (hasNext())
            conditionTypes.addAll(next.conditionTypes());
        return conditionTypes;
    }

    @Override
    public String toString() {
        return columnName + " " + conditionType + " " + params;
    }
}
