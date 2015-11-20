package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Project implements RelationalOperator {

    private final List<String> colNames;
    private final RelationalOperator operator;

    public Project(List<String> colNames, RelationalOperator operator) {
        this.colNames = colNames;
        this.operator = operator;
    }

    @Override
    public Row next() {
        Row next = operator.next();
        return new Row(colNames.stream()
                .map(s -> new SimpleEntry<>(s, next.getValue(s)))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
    }

    @Override
    public boolean hasNext() {
        return operator.hasNext();
    }

    @Override
    public void reset() {
        operator.reset();
    }
}
