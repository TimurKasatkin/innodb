package ru.innopolis.dmd.project.innodb.scheme.index.impl;

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

import static ru.innopolis.dmd.project.innodb.Cache.getTable;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.TABLE_PAGES_COUNT;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.entry;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.map;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.setToPage;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.getPage;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.format;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.hash;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class HashPKIndex extends AbstractIndex<String, Row> implements PKIndex {

    private int pageNum;

    private RandomAccessFile raf;

    public HashPKIndex(int pageNum, List<Column> columns) {
        super(columns);
        this.pageNum = pageNum;
        try {
            raf = new RandomAccessFile(DBConstants.DB_FILE, "rw");
            setToPage(raf, pageNum);
            if (!PageType.byMarker((char) raf.readByte()).equals(PageType.TABLE_SCHEME)) {
                throw new IllegalArgumentException("There is no table scheme for pk index on page " + pageNum);
            }
            setToPage(raf, pageNum);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Table articles = getTable("articles");
        PKIndex pkIndex = articles.getPkIndex();
//        Row row = new Row(map(entry("id", 5),
//                entry("title", "lol title"),
//                entry("publtype", "journal_article"),
//                entry("url", "urlka"),
//                entry("year", null)));
//        pkIndex.insert("5", row);
        for (int i = 0; i < 200; i++) {
            Row row = new Row(map(entry("id", i),
                    entry("title", "lol title#" + i),
                    entry("publtype", "journal_article"),
                    entry("url", "urlka #" + i),
                    entry("year", i)));
            pkIndex.insert(i + "", row);
        }
    }

    @Override
    public Row search(String pkStr) {
        try {
            int pageNum = hash(pkStr) % TABLE_PAGES_COUNT;
            pageNum = this.pageNum + 1 + pageNum;
            TableDataPage page = (TableDataPage) getPage(pageNum, raf);
            setToPage(raf, this.pageNum);
            int freeOffset = page.getFreeOffset();
            int nextPageNum = page.getNextPageNum();

            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void insert(String pkStr, Row row) {
//        try {
        int pageNum = hash(pkStr) % TABLE_PAGES_COUNT;
        String formattedRow = format(row);
//            setToPage(raf, pageNum + this.pageNum + 1);
        TableDataPage page = (TableDataPage) getPage(pageNum + this.pageNum + 1, raf);
        boolean inserted = false;
        TableDataPage cur = page;
        while (!inserted) {
            if (cur.canInsert(formattedRow)) {
                cur.insert(formattedRow);
                cur.serialize(raf);
                inserted = true;
            } else {
                if (cur.hasNext())
                    cur = cur.next();
                else {

                }
            }
        }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void remove(String s) {

    }

}
