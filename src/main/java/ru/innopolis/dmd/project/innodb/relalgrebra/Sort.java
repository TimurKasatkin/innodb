package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.list;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Sort implements RelationalOperator {

    private final String sortBy;
    private final RelationalOperator operator;
    private SortType sortType = SortType.ASC;
    private int offset = -1;
    private int limit = -1;
    private Iterator<Row> rowsIterator;
    private int rowCount = 0;

    public Sort(String sortBy, RelationalOperator operator) {
        this(sortBy, SortType.ASC, operator);
    }

    public Sort(String sortBy, SortType sortType, RelationalOperator operator) {
        this.sortBy = sortBy;
        this.operator = operator;
        List<Row> list = operator.loadAll();
        Collections.sort(list, (o1, o2) -> (sortType.equals(SortType.ASC) ? 1 : -1) *
                o1.v(sortBy).compareTo(o2.v(sortBy)));
        rowsIterator = list.iterator();
        rowCount = 0;
    }

    public Sort(String sortBy, RelationalOperator operator, int offset, int limit) {
        this(sortBy, SortType.ASC, operator, offset, limit);
    }

    public Sort(String sortBy, SortType sortType, RelationalOperator operator, int offset, int limit) {
        if (offset < 0)
            throw new IllegalArgumentException("Offset should not be negative");
        if (limit <= 0)
            throw new IllegalArgumentException("Offset should not be negative or zero");
        List<Row> list = operator.loadAll();
        Collections.sort(list, (o1, o2) -> (sortType.equals(SortType.ASC) ? 1 : -1) *
                o1.v(sortBy).compareTo(o2.v(sortBy)));
        rowsIterator = list.iterator();
        this.sortBy = sortBy;
        int i = 0;
        while (i < offset && hasNext()) {
            next();
            i++;
        }
        rowCount = 0;
        this.operator = operator;
        this.offset = offset;
        this.limit = limit;
    }

    public static void main(String[] args) {
        simpleTest();
//        testWithOffsetAndLimit(1, 5, SortType.ASC, "url");
    }

    private static void testWithOffsetAndLimit(int offset, int limit, SortType sortType, String sortBy) {
        Sort sort = new Sort(sortBy, sortType, new Scan("articles"), offset, limit);
        List<Row> rows = new LinkedList<>();
        while (sort.hasNext()) {
            Row next = sort.next();
            rows.add(next);
            System.out.println(next);
        }
        System.out.println(MessageFormat.format("Offset: {0}; Limit: {1}; Rows count: {2}", offset, limit, rows.size()));
    }

    private static void simpleTest() {
        Sort sort = new Sort("id", SortType.DESC, new Project(list("id", "title"), new Scan("articles")));
        while (sort.hasNext())
            System.out.println(sort.next());
        sort.reset();
        System.out.println(sort.loadAll().size());
    }

    @Override
    public Row next() {
        if (hasLimit() && rowCount >= limit)
            return null;
        rowCount++;
        return rowsIterator.next();
    }

    @Override
    public boolean hasNext() {
        boolean res = rowsIterator.hasNext();
        if (hasLimit())
            res = res && rowCount < limit;
        return res;
    }

    @Override
    public void reset() {
        operator.reset();
        List<Row> list = operator.loadAll();
        Collections.sort(list, (o1, o2) -> (sortType.equals(SortType.ASC) ? 1 : -1) *
                o1.v(sortBy).compareTo(o2.v(sortBy)));
        rowsIterator = list.iterator();
        if (hasOffset()) {
            int offset = this.offset;
            int limit = this.limit;
            this.offset = -1;
            this.limit = -1;
            int i = 0;
            while (i < offset && hasNext()) {
                next();
                i++;
            }
            this.offset = offset;
            this.limit = limit;
        }
        rowCount = 0;
    }

    private boolean hasOffset() {
        return offset != -1;
    }

    private boolean hasLimit() {
        return limit != -1;
    }

    public enum SortType {
        ASC, DESC
    }
}
