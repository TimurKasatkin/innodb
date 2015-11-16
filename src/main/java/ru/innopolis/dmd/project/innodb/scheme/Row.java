package ru.innopolis.dmd.project.innodb.scheme;

import ru.innopolis.dmd.project.innodb.db.DBConstants;

import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Row {

    private Map<String, Comparable> entries;

    public Row(Map<String, Comparable> entries) {
        this.entries = entries;
    }

    public Row(Entry<String, Comparable>... entries) {
        this.entries = Stream.of(entries)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public static List<Row> parseRows(List<Column> columns, String rowStr) {
        List<Row> rows = new LinkedList<>();
        Matcher matcher = DBConstants.ROW_REGEXP.matcher(rowStr);
        while (matcher.find()) {
            Map<String, Comparable> rowMap = new LinkedHashMap<>();
            String[] split = matcher.group(1).split("\\$");
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                rowMap.put(column.getName(), column.getType().parse(split[i]));
            }
            rows.add(new Row(rowMap));
        }
        return rows;
    }

    public Comparable getValue(String fieldName) {
        return entries.get(fieldName);
    }

    public void setValue(String fieldName, Comparable value) {
        entries.put(fieldName, value);
    }

    public Map<String, Comparable> getEntries() {
        return entries;
    }

    public Collection<Comparable> getValues() {
        return entries.values();
    }

    public Set<String> getColumnNames() {
        return entries.keySet();
    }

    @Override
    public String toString() {
        return "[" + entries.entrySet().stream().map(Entry::toString).collect(Collectors.joining(",")) + "]";
    }
}
