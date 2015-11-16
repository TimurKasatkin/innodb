package ru.innopolis.dmd.project.innodb.sql;

import ru.innopolis.dmd.project.innodb.scheme.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 24.10.15.
 * @email aronwest001@gmail.com
 */
public class Condition {

    private String columnName;

    private ConditionType conditionType;

    private List<Comparable> params;

    public Condition(String columnName, ConditionType conditionType, Comparable... params) {
        this(columnName, conditionType, Arrays.asList(params));
    }

    public Condition(String columnName, ConditionType conditionType, List<Comparable> params) {
        this.columnName = columnName;
        this.conditionType = conditionType;
        this.params = params;
    }

    public boolean matches(Row row) {
        return conditionType.matches(row.getValue(columnName), params);
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
