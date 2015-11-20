package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Rename implements RelationalOperator {

    private final Map<String, String> newNamesMap;
    private final RelationalOperator operator;

    public Rename(Map<String, String> newNamesMap, RelationalOperator operator) {
        //old name -> new name
        this.newNamesMap = newNamesMap;
        this.operator = operator;
    }

    @Override
    public Row next() {
        Row next = operator.next();
        Map<String, Comparable> newRowMap = new HashMap<>();
        for (Map.Entry<String, Comparable> entry : next.entries().entrySet()) {
            String newName = newNamesMap.get(entry.getKey());
            newRowMap.put(newName != null ? newName : entry.getKey(), entry.getValue());
        }
        return new Row(newRowMap);
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
