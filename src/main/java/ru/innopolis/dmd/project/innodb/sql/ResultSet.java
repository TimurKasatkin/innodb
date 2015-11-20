package ru.innopolis.dmd.project.innodb.sql;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * @author Timur Kasatkin
 * @date 17.11.15.
 * @email aronwest001@gmail.com
 */
public class ResultSet {

    private Set<String> names;

    private List<List<Comparable>> rows;

    public ResultSet(List<Row> rows) {
        this.names = rows.get(0).getColumnNames();
        this.rows = rows.stream()
                .map(Row::getValues)
                .map(vals -> vals.stream().collect(toList()))
                .collect(toList());
    }

    public Set<String> getNames() {
        return names;
    }

    public List<List<Comparable>> getRows() {
        return rows;
    }
}
