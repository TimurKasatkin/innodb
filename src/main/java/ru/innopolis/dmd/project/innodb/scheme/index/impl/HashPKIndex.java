package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.page.PageType;
import ru.innopolis.dmd.project.innodb.db.page.TableDataPage;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.stream.Collectors;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.TABLE_PAGES_COUNT;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.setToPage;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.*;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.format;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.pkValue;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class HashPKIndex extends AbstractIndex<String, Row> implements PKIndex {

    private int pageNum;

    private RandomAccessFile raf;

    public HashPKIndex(int pageNum, String tableName, List<Column> columns) {
        super(tableName, columns);
        this.pageNum = pageNum;
        try {
            raf = new RandomAccessFile(DBConstants.DB_FILE, "rw");
            assertPageTypesEquals(raf, pageNum, PageType.TABLE_SCHEME);
            setToPage(raf, pageNum);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Table journals = Cache.getTable("journals");
        PKIndex pkIndex = journals.getPkIndex();
//        long start = System.nanoTime();
//        for (int i = 1; i < 10000; i++) {
//            Row row = new Row(map(entry("id", i),
//                    entry("title", "lol title#" + i),
//                    entry("publtype", "journal_article"),
//                    entry("url", "urlka #" + i),
//                    entry("year", i)));
//            pkIndex.insert(i + "", row);
//        }
//        System.out.println("Inserted for " + (System.nanoTime() - start) / 1000000 + " ns.");
//        for (int i = 0; i < 10000; i++) {
//            System.out.println("Row with key " + i + ":" + pkIndex.search(i + ""));
//        }
        pkIndex.insert("5", new Row(
                map(entry("id", 5), entry("name", "name #5"))
        ));
    }

    @Override
    public Row search(String pkStr) {
        int pageNum = pageNumber(pkStr, TABLE_PAGES_COUNT);
        pageNum = this.pageNum + 1 + pageNum;
        TableDataPage cur = (TableDataPage) getPage(pageNum, raf);
        do {
            Row row = stream(cur.getRows(getTable().getName()))
                    .filter(r -> pkValue(r, table).equals(pkStr))
                    .findFirst().orElse(null);
            if (row != null) return row;
            cur = cur.next();
        } while (cur != null);
        return null;
    }

    private int findPageNum(String pkStr) {
        int pageNum = pageNumber(pkStr, TABLE_PAGES_COUNT);
        pageNum = this.pageNum + 1 + pageNum;
        TableDataPage cur = (TableDataPage) getPage(pageNum, raf);
        do {
            Row row = stream(cur.getRows(getTable().getName()))
                    .filter(r -> pkValue(r, table).equals(pkStr))
                    .findFirst().orElse(null);
            if (row != null) return cur.getNumber();
            cur = cur.next();
        } while (cur != null);
        return -1;
    }

    @Override
    public void insert(String pkStr, Row row) {
        int pageNum = pageNumber(pkStr, TABLE_PAGES_COUNT);
        String formattedRow = format(row);
        boolean inserted = false;
        System.out.print("Trying to insert: " + formattedRow + " ... ");
        TableDataPage cur = (TableDataPage) getPage(pageNum + this.pageNum + 1, raf);
        int newRowPageNum = 0;
        while (!inserted) {
            if (cur.canInsert(formattedRow)) {
                cur.insert(formattedRow);
                newRowPageNum = cur.getNumber();
                cur.serialize(raf);
                inserted = true;
            } else {
                if (cur.hasNext())
                    cur = cur.next();
                else {
                    TableDataPage newPage = (TableDataPage) createPage(PageType.TABLE_DATA, raf);
                    cur.setNextPageNum(newPage.getNumber());
                    cur.serialize(raf);
                    newPage.insert(formattedRow);
                    newPage.serialize(raf);
                    newRowPageNum = cur.getNumber();
                    inserted = true;
                }
            }
        }
        for (Index<String, Integer> uniqueIndexes : getTable().getUniqueIndexes()) {
            uniqueIndexes.insert(stream(uniqueIndexes.getColumns())
                    .map(Column::getName)
                    .map(row::getValue)
                    .map(Object::toString)
                    .collect(Collectors.joining()), newRowPageNum);
        }
        System.out.println("OK.");
    }

    @Override
    public void remove(String s) {
        int pageNum = pageNumber(s, TABLE_PAGES_COUNT);
        System.out.println("Trying to delete row with key: '" + s + "' ...");
        pageNum = this.pageNum + 1 + pageNum;
        TableDataPage cur = (TableDataPage) getPage(pageNum, raf);
        do {
            Row row = stream(cur.getRows(getTable().getName()))
                    .filter(r -> pkValue(r, table).equals(s))
                    .findFirst().orElse(null);
            if (row != null) {
                String formattedRow = format(row);
                if (cur.has(formattedRow)) {
                    cur.delete(formattedRow);
                    cur.serialize(raf);
                    System.out.println("OK.");
                    for (Index<String, Integer> uniqueIndex : getTable().getUniqueIndexes()) {
                        uniqueIndex.remove(stream(uniqueIndex.getColumns())
                                .map(Column::getName)
                                .map(row::getValue)
                                .map(Object::toString)
                                .collect(Collectors.joining()));
                    }
                    return;
                }
            }
            cur = cur.next();
        } while (cur != null);
        System.out.println("NOT FOUND.");
    }


}
