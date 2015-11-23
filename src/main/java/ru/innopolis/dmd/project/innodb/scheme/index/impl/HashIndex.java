package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.page.IndexDataPage;
import ru.innopolis.dmd.project.innodb.db.page.PageType;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.utils.PageUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.INDEX_PAGE_COUNT;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.*;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.format;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class HashIndex extends AbstractIndex<String, Long> {

    private int pageNum;

    private RandomAccessFile raf;

    public HashIndex(int pageNum, String tableName, List<Column> columns) {
        super(tableName, columns);
        this.pageNum = pageNum;
        try {
            raf = new RandomAccessFile(DBConstants.DB_FILE, "rw");
            PageUtils.assertPageTypesEquals(raf, pageNum, PageType.INDEX_SCHEME);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long search(String s) {
        int pageNum = pageNumber(s, INDEX_PAGE_COUNT);
        pageNum = this.pageNum + 1 + pageNum;
        IndexDataPage cur = (IndexDataPage) getPage(pageNum, raf);
        do {
            Row row = stream(cur.getRows())
                    .filter(r -> r.getValue("key").equals(s))
                    .findFirst().orElse(null);
            if (row != null) return (Long) row.v("val");
            cur = cur.next();
        } while (cur != null);
        return null;
    }

    @Override
    public void insert(String s, Long pageNumber) {
        int pageNum = pageNumber(s, INDEX_PAGE_COUNT);
        String formattedRow = format(new Row(map(
                entry("key", s),
                entry("val", pageNumber)
        )));
        boolean inserted = false;
        System.out.print("Trying to insert index row: " + formattedRow + " ... ");
        IndexDataPage cur = (IndexDataPage) getPage(pageNum + this.pageNum + 1, raf);
        while (!inserted) {
            if (cur.canInsert(formattedRow)) {
                cur.insert(formattedRow);
                cur.serialize(raf);
                inserted = true;
            } else {
                if (cur.hasNext())
                    cur = cur.next();
                else {
                    IndexDataPage newPage = (IndexDataPage) createPage(PageType.INDEX_DATA, raf);
                    cur.setNextPageNum(newPage.getNumber());
                    cur.serialize(raf);
                    newPage.insert(formattedRow);
                    newPage.serialize(raf);
                    inserted = true;
                }
            }
        }
        System.out.println("OK.");
    }

    @Override
    public void remove(String s) {
        int pageNum = pageNumber(s, INDEX_PAGE_COUNT);
        System.out.println("Trying to delete index row with key: '" + s + "' ...");
        pageNum = this.pageNum + 1 + pageNum;
        IndexDataPage cur = (IndexDataPage) getPage(pageNum, raf);
        do {
            Row row = stream(cur.getRows())
                    .filter(r -> r.getValue("key").equals(s))
                    .findFirst().orElse(null);
            if (row != null) {
                String formattedRow = format(row);
                if (cur.has(formattedRow)) {
                    cur.delete(formattedRow);
                    cur.serialize(raf);
                    System.out.println("OK.");
                    return;
                }
            }
            cur = cur.next();
        } while (cur != null);
        System.out.println("NOT FOUND.");
    }
}
