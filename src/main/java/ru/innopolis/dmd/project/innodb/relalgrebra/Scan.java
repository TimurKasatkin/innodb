package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.page.TableDataPage;
import ru.innopolis.dmd.project.innodb.scheme.Table;

import java.io.RandomAccessFile;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static ru.innopolis.dmd.project.innodb.utils.FileUtils.closeRaf;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.createRaf;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.getPage;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Scan implements RelationalOperator {

    private Table table;
    private int offset = -1;
    private int limit = -1;

    private Queue<Integer> pageNumsQueue = new LinkedList<>();

    private Iterator<Row> currentPageIterator;

    private int rowCount = 0;
    private RandomAccessFile raf;
    private String tableName;

    public Scan(String tableName, int offset, int limit) {
        this(Cache.getTable(tableName), offset, limit);
    }

    public Scan(Table table, int offset, int limit) {
        if (offset < 0)
            throw new IllegalArgumentException("Offset should not be negative");
        if (limit <= 0)
            throw new IllegalArgumentException("Offset should not be negative or zero");
        initScan(table);
        int i = 0;
        while (i < offset && hasNext()) {
            next();
            i++;
        }
        rowCount = 0;
        this.offset = offset;
        this.limit = limit;
    }

    public Scan(String tableName) {
        this(Cache.getTable(tableName));
    }


    public Scan(Table table) {
        initScan(table);
    }

    public static void main(String[] args) {
        fullScanTest();
//        testWithOffsetAndLimit(0, 5);
//        testWithOffsetAndLimit(5, 5);
//        testWithOffsetAndLimit(10, 5);
//        testWithOffsetAndLimit(100000, 5);
//        testWithOffsetAndLimit(0, 9999);
    }

    private static void fullScanTest() {
        Scan articles = new Scan("users");
        List<Row> rows = articles.loadAll();
        System.out.println("Rows count: " + rows.size());
        rows.forEach(System.out::println);
        articles.reset();
        System.out.println(articles.loadAll().size());
        articles.reset();
        int i = 0;
        int limit = 50;
        while (i < limit && articles.hasNext()) {
            System.out.println(articles.next());
            i++;
        }
        articles.reset();
        System.out.println(articles.loadAll().size());
    }

    private static void testWithOffsetAndLimit(int offset, int limit) {
        Scan articles = new Scan("articles", offset, limit);
        List<Row> rows = articles.loadAll();
        System.out.println(MessageFormat.format("Offset: {0}; Limit: {1}; Rows count: {2}", offset, limit, rows.size()));
        rows.forEach(System.out::println);
        articles.reset();
        System.out.println("After reset:");
        List<Row> rowsAfterReset = new LinkedList<>();
        while (articles.hasNext()) {
            Row next = articles.next();
            rowsAfterReset.add(next);
            System.out.println(next);
        }
        assert rows.equals(rowsAfterReset);
    }

    @Override
    public Row next() {
        if (hasLimit() && rowCount >= limit)
            return null;
        Row result = null;
        boolean iterHasNext = currentPageIterator.hasNext();
        boolean queueNotEmpty = !pageNumsQueue.isEmpty();
        if (iterHasNext || queueNotEmpty) {
            rowCount++;
            if (iterHasNext)
                result = currentPageIterator.next();
            else
                do {
                    TableDataPage page = (TableDataPage) getPage(pageNumsQueue.poll(), raf);
                    if (page.hasNext())
                        pageNumsQueue.add(page.getNextPageNum());
                    currentPageIterator = page.getRows(tableName).iterator();
                    if (currentPageIterator.hasNext())
                        result = currentPageIterator.next();
                } while (!pageNumsQueue.isEmpty() && result == null);
        }
        return result;
    }

    @Override
    public boolean hasNext() {
        boolean res = currentPageIterator.hasNext() || !pageNumsQueue.isEmpty();
        if (hasLimit())
            res = res && rowCount < limit;
        if (!res) {
            closeRaf(raf);
            raf = null;
        }
        return res;
    }

    @Override
    public void reset() {
        if (raf == null)
            raf = createRaf("r");
        pageNumsQueue.clear();
        int firstDataPageNum = table.getPageNumber() + 1;
        for (int i = firstDataPageNum; i < firstDataPageNum + DBConstants.TABLE_PAGES_COUNT; i++) {
            pageNumsQueue.add(i);
        }
        TableDataPage page = (TableDataPage) getPage(pageNumsQueue.poll(), raf);
        if (page.hasNext())
            pageNumsQueue.add(page.getNextPageNum());
        currentPageIterator = page.getRows(tableName).iterator();
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

    private void initScan(Table table) {
        if (table == null)
            throw new IllegalArgumentException("There is no table");
        this.table = table;
        this.tableName = table.getName();
        this.raf = createRaf("r");
        int firstDataPageNum = table.getPageNumber() + 1;
        for (int i = firstDataPageNum; i < firstDataPageNum + DBConstants.TABLE_PAGES_COUNT; i++) {
            pageNumsQueue.add(i);
        }
        TableDataPage page = (TableDataPage) getPage(pageNumsQueue.poll(), raf);
        if (page.hasNext())
            pageNumsQueue.add(page.getNextPageNum());
        currentPageIterator = page.getRows(tableName).iterator();
    }

    private boolean hasOffset() {
        return offset != -1;
    }

    private boolean hasLimit() {
        return limit != -1;
    }
}
