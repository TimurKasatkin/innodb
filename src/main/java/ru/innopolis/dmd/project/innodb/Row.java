package ru.innopolis.dmd.project.innodb;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.stream;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Row {

    //col name -> value
    private Map<String, Comparable> entries;

    public Row(Map<String, Comparable> entries) {
        this.entries = entries;
    }

    public boolean has(String fieldName) {
        return entries.containsKey(fieldName);
    }

    public Comparable v(String fieldName) {
        return getValue(fieldName);
    }

    public Comparable getValue(String fieldName) {
        return entries.get(fieldName);
    }

    public void setValue(String fieldName, Comparable value) {
        entries.put(fieldName, value);
    }

    public Map<String, Comparable> entries() {
        return entries;
    }

    public Set<String> getColumnNames() {
        return entries.keySet();
    }

    public Collection<Comparable> getValues() {
        return entries.values();
    }

    @Override
    public String toString() {
        return "[" + stream(entries.entrySet()).map(Entry::toString).collect(joining(",")) + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return !(entries != null ? !entries.equals(row.entries) : row.entries != null);
    }

    @Override
    public int hashCode() {
        return entries != null ? entries.hashCode() : 0;
    }
}
