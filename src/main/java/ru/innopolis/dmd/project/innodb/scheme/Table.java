package ru.innopolis.dmd.project.innodb.scheme;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.PageType;
import ru.innopolis.dmd.project.innodb.scheme.constraint.Constraint;
import ru.innopolis.dmd.project.innodb.scheme.constraint.PrimaryKey;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;
import ru.innopolis.dmd.project.innodb.scheme.index.MultivaluedIndex;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;
import ru.innopolis.dmd.project.innodb.utils.FileUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
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

    private List<Index<String, Long>> uniqueIndexes;

    private List<MultivaluedIndex<String, Long>> multivaluedIndexes;

    private List<Table> childTables;

    private List<Table> parentTables;

    private int pageNumber;

    @SafeVarargs
    public Table(int pageNumber,
                 String name,
                 List<Column> primaryKey,
                 List<Column> columns,
                 List<Constraint> constraints,
                 PKIndex pkIndex,
                 Index<String, Long>... uniqueIndexes) {
//        if (!pks.stream().allMatch(columns::contains))
//            throw new IllegalArgumentException("List of columns should contain all primary keys");
        try {
            RandomAccessFile raf = new RandomAccessFile(DBConstants.DB_FILE, "r");
            FileUtils.setToPage(raf, pageNumber);
            if (!PageType.byMarker((char) raf.readByte()).equals(PageType.TABLE_SCHEME))
                throw new IllegalArgumentException("There is no '" + name + "' table desciption on page " + pageNumber);
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
            this.pageNumber = pageNumber;
            this.pkIndex = pkIndex;
            this.name = name;
            this.primaryKeys = primaryKey;
            this.columns = Collections.unmodifiableList(columns);
            this.constraints = constraints;
            this.uniqueIndexes = list(uniqueIndexes);
            this.multivaluedIndexes = new LinkedList<>();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    public List<Index<String, Long>> getUniqueIndexes() {
        return uniqueIndexes;
    }

    public PKIndex getPkIndex() {
        return pkIndex;
    }

    public void addMultivaluedIndex(MultivaluedIndex<String, Long> multivaluedIndex) {
        if (multivaluedIndex == null) {
            throw new IllegalArgumentException("Multivalued index null!!!");
        }
        multivaluedIndexes.add(multivaluedIndex);
    }

    public void addParentTable(Table table) {
        if (parentTables == null)
            synchronized (this) {
                if (parentTables == null) parentTables = new LinkedList<>();
            }
        parentTables.add(table);
    }

    public void addChildTable(Table table) {
        if (childTables == null)
            synchronized (this) {
                if (childTables == null) childTables = new LinkedList<>();
            }
        childTables.add(table);
    }
}
