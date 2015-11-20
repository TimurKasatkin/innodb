package ru.innopolis.dmd.project.innodb.utils;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.scheme.constraint.Constraint;
import ru.innopolis.dmd.project.innodb.scheme.type.Types;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class SchemeUtils {

//    private static Column parseColumn(String colDescription) {
//        colDescription = colDescription.substring(
//                isPk(colDescription) ? PRIMARY_KEY_MARKER.length() + 1 : 1,
//                colDescription.length() - 1);
//        String[] split = colDescription.split(MAIN_DELIMITER_REGEXP);
//        return new Column(split[0], Types.byName(split[1]));
//    }

    public static Table parseTable(String schemeDescription) {
        Matcher matcher = TABLE_SCHEME_REGEXP.matcher(schemeDescription);
        if (matcher.find()) {
            String tableName = matcher.group("tablename");
            String descriptionStr = matcher.group("descr");
            String constraintsStr = matcher.group("constraints");
            String indexesStr = matcher.group("indexes");

            List<Column> columns = parseColumns(descriptionStr);
            List<String> columnNames = parallelStream(columns).map(Column::getName).collect(toList());
            List<Column> pks = new LinkedList<>();
            //PARSE INDEXES


            //PARSE CONSTRAINTS
            matcher = TABLE_CONSTRAINT_REGEXP.matcher(constraintsStr);
            while (matcher.find()) {
                String[] colNames = matcher.group("constraintcols").split(",");
                if (!columnNames.containsAll(list(colNames))) {
                    throw new IllegalArgumentException("Constraints description has columns which are not in ");
                }
                String[] constraintStrs = matcher.group("constraints").split(",");
                List<Constraint> constraints = new LinkedList<>();
                for (String constraintStr : constraintStrs) {
                    switch (constraintStr) {
                        case "pk":
//                            constraints.add(new PrimaryKey());
                            break;
                        case "fk":
                        case "unique":

                            break;
                        case "notnull":
//                            constraints.addAll(Stream.of());
                            break;
                    }
                }
                System.out.println();
            }
            System.out.println();
        }
        return null;
    }

    public static List<Column> parseColumns(String colsDescription) {
        List<Column> columns = new LinkedList<>();
        Matcher matcher = TABLE_COL_SCHEME_REGEXP.matcher(colsDescription);
        while (matcher.find())
            columns.add(new Column(matcher.group("colname"), Types.byName(matcher.group("coltype"))));
        return columns;
    }

    public static Column parseColumn(String colDescription) {
        Matcher matcher = TABLE_COL_SCHEME_REGEXP.matcher(colDescription);
        if (matcher.matches())
            return new Column(matcher.group("colname"), Types.byName(matcher.group("coltype")));
        else
            throw new IllegalArgumentException("Invalid column description format");
    }

    public static List<Row> parseRows(List<Column> columns, String rowsStr) {
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

    public static Row parseRow(List<Column> columns, String rowStr) {
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

    public static String formatRow(Row row) {
        return "[" + stream(row.entries().entrySet()).map(Map.Entry::toString).collect(joining(",")) + "]";
    }

}
