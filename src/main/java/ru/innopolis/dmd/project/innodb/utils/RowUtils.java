package ru.innopolis.dmd.project.innodb.utils;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.ROW_REGEXP;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;

/**
 * @author Timur Kasatkin
 * @date 21.11.15.
 * @email aronwest001@gmail.com
 */
public class RowUtils {

    public static String pkValue(Row row, List<Column> pkColumns) {
        return stream(pkColumns)
                .map(Column::getName)
                .map(row::getValue)
                .map(Object::toString)
                .collect(joining());
    }

    public static String pkValue(Row row, Table table) {
        return pkValue(row, table.getPrimaryKeys());
    }

    public static List<Row> parseRows(String rowsStr, List<Column> columns) {
        List<Row> rows = new LinkedList<>();
        Matcher matcher = ROW_REGEXP.matcher(rowsStr);
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

    public static Row parseRow(String rowStr, List<Column> columns) {
        Matcher matcher = ROW_REGEXP.matcher(rowStr);
        if (matcher.matches()) {
            Map<String, Comparable> rowMap = new LinkedHashMap<>();
            String[] split = matcher.group(1).split("\\$");
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                rowMap.put(column.getName(), column.getType().parse(split[i]));
            }
            return new Row(rowMap);
        }
        return null;
    }

    public static String format(Row row) {
        return "[" + stream(row.getValues())
                .map(val -> val == null ? "\\NUL" : val.toString())
                .collect(Collectors.joining("$")) + "]";
    }

    public static void main(String[] args) {
        Row row = new Row(map(entry("id", 5),
                entry("title", "lol title"),
                entry("publtype", "journal_article"),
                entry("url", "urlka"),
                entry("year", null)));
        System.out.println("ROW: " + row);
        String format = format(row);
        System.out.println("FORMATTED ROW: " + format);
        System.out.println("PARSED ROW: " + parseRow(format, Cache.getTable("articles").getColumns()));
    }

}
