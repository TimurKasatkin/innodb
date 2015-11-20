package ru.innopolis.dmd.project.innodb.scheme;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.constraint.Constraint;
import ru.innopolis.dmd.project.innodb.scheme.constraint.PrimaryKey;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.list;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Table {

    private String name;

    private PKIndex pkIndex;

    private List<Column> primaryKeys;

    private List<Column> columns;

    private List<Constraint> constraints;

    private List<Index<String, Long>> indexes;

    private int pageNumber;

    @SafeVarargs
    public Table(String name,
                 List<Column> primaryKey,
                 List<Column> columns,
                 List<Constraint> constraints,
                 PKIndex pkIndex,
                 Index<String, Long>... indexes) {
//        if (!pks.stream().allMatch(columns::contains))
//            throw new IllegalArgumentException("List of columns should contain all primary keys");
        if (constraints.parallelStream().noneMatch(constraint -> constraint instanceof PrimaryKey)) {
            throw new IllegalArgumentException("Table should contain primary key constraint");
        }
        Set<Column> constraintColumns = constraints.stream()
                .map(Constraint::getColumns)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        if (!constraintColumns.parallelStream().allMatch(columns::contains)) {
            throw new IllegalArgumentException("Constraints contains columns which are not in table");
        }
        this.pkIndex = pkIndex;
        this.name = name;
        this.primaryKeys = primaryKey;
        this.columns = Collections.unmodifiableList(columns);
        this.constraints = constraints;
        this.indexes = list(indexes);
    }

    public boolean test(Row row) {
        return constraints.stream().allMatch(c -> c.test(row));
    }

    public Column getColumn(String columnName) {
        return columns.parallelStream()
                .filter(col -> col.getName().equalsIgnoreCase(columnName))
                .findFirst().orElse(null);
    }

//    public void addIndex(String columnName, MultivaluedBPlusTreeIndex<String, Long> tree) {
//        Column column = getColumn(columnName);
//        if (column == null)
//            throw new IllegalArgumentException();
//        indexesMap.put(columnName, tree);
//    }

    public List<Column> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public List<Index<String, Long>> getIndexes() {
        return indexes;
    }
}
