package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.PageType;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;

import java.io.IOException;
import java.io.RandomAccessFile;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.INDEX_PAGE_COUNT;
import static ru.innopolis.dmd.project.innodb.utils.StorageUtils.setToPage;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.hash;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class PKHashIndex implements Index<String, Row> {

    private int pageNum;

    private RandomAccessFile raf;

    public PKHashIndex(int pageNum) {
        this.pageNum = pageNum;
        try {
            raf = new RandomAccessFile(DBConstants.DB_FILE, "rw");
            setToPage(raf, pageNum);
            if (!PageType.byMarker((char) raf.readByte()).equals(PageType.INDEX_SCHEME))
                throw new IllegalArgumentException("Page with specified number is not index scheme page");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row search(String pkStr) {
        try {
            int pageNum = hash(pkStr) % INDEX_PAGE_COUNT;
            setToPage(raf, this.pageNum + 1 + pageNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void insert(String pkStr, Row row) {

    }

    @Override
    public void remove(String s) {

    }
}
