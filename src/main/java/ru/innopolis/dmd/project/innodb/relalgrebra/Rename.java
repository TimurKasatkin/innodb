package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.LinkedHashMap;
import java.util.Map;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;

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

    public static void main(String[] args) {
        Rename rename = new Rename(map(entry("id", "lol_id"), entry("title", "lol_title")), new Project(list("id", "title"), new Scan("articles")));
        while (rename.hasNext())
            System.out.println(rename.next());
    }

    @Override
    public Row next() {
        Row next = operator.next();
        Map<String, Comparable> newRowMap = new LinkedHashMap<>();
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
