package ru.innopolis.dmd.project.innodb.scheme;

import ru.innopolis.dmd.project.innodb.db.index.BPlusTree;
import ru.innopolis.dmd.project.innodb.scheme.type.Types;

import java.util.*;
import java.util.regex.Matcher;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Table {

    private String name;

    private List<Column> primaryKeys;

    private List<Column> columns;

    /**
     * Column name -> B+ Tree for this column
     */
    private Map<String, BPlusTree<String, Long>> indexes;

    public Table(String name, List<Column> pks, List<Column> columns) {
        if (!pks.stream().allMatch(columns::contains))
            throw new IllegalArgumentException("List of columns should contain all primary keys");
        this.name = name;
        this.primaryKeys = Collections.unmodifiableList(pks);
        this.columns = Collections.unmodifiableList(columns);
        this.indexes = new HashMap<>();
    }

    public static Table parseTable(String tableName, String schemeDescription) {
        List<Column> pks = new LinkedList<>();
        List<Column> columns = new LinkedList<>();
        Matcher matcher = COL_DESCRIPTION_REGEXP.matcher(schemeDescription);
        while (matcher.find()) {
            String group = matcher.group(1);
            Column column = parseColumn(group);
            columns.add(column);
            if (isPk(group)) pks.add(column);
        }
        return new Table(tableName, pks, columns);
    }

    private static boolean isPk(String columnDescription) {
        return columnDescription.contains(PRIMARY_KEY_MARKER);
    }

    private static Column parseColumn(String colDescription) {
        colDescription = colDescription.substring(
                isPk(colDescription) ? PRIMARY_KEY_MARKER.length() + 1 : 1,
                colDescription.length() - 1);
        String[] split = colDescription.split(MAIN_DELIMITER_REGEXP);
        return new Column(split[0], Types.byName(split[1]));
    }

    public Column getColumn(String columnName) {
        return columns.parallelStream()
                .filter(col -> col.getName().equalsIgnoreCase(columnName))
                .findFirst().orElse(null);
    }

    public void addIndex(String columnName, BPlusTree<String, Long> tree) {
        Column column = getColumn(columnName);
        if (column == null)
            throw new IllegalArgumentException();
        indexes.put(columnName, tree);
    }

    public List<Column> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    public Map<String, BPlusTree<String, Long>> getIndexes() {
        return indexes;
    }
}
