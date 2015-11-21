package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.PageType;
import ru.innopolis.dmd.project.innodb.db.page.TableDataPage;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.TABLE_PAGES_COUNT;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.stream;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.setToPage;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.*;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.format;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.pkValue;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.hash;

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
        Table articles = Cache.getTable("articles");
        PKIndex pkIndex = articles.getPkIndex();
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
        for (int i = 0; i < 10000; i++) {
            System.out.println("Row with key " + i + ":" + pkIndex.search(i + ""));
        }
    }

    @Override
    public Row search(String pkStr) {
        int pageNum = hash(pkStr) % TABLE_PAGES_COUNT;
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

    @Override
    public void insert(String pkStr, Row row) {
        int pageNum = hash(pkStr) % TABLE_PAGES_COUNT;
        String formattedRow = format(row);
        boolean inserted = false;
        System.out.print("Trying to insert: " + formattedRow + " ... ");
        TableDataPage cur = (TableDataPage) getPage(pageNum + this.pageNum + 1, raf);
        while (!inserted) {
            if (cur.canInsert(formattedRow)) {
                cur.insert(formattedRow);
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
                    inserted = true;
                }
            }
        }
        System.out.println("OK.");
    }

    @Override
    public void remove(String s) {

    }

}
