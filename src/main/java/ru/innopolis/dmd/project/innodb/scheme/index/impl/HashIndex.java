package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.PageType;
import ru.innopolis.dmd.project.innodb.scheme.Column;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static ru.innopolis.dmd.project.innodb.utils.FileUtils.setToPage;

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
            setToPage(raf, pageNum);
            if (!PageType.byMarker((char) raf.readByte()).equals(PageType.INDEX_SCHEME)) {
                throw new IllegalArgumentException("There is no index scheme on page " + pageNum);
            }
            setToPage(raf, pageNum);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long search(String s) {

        return null;
    }

    @Override
    public void insert(String s, Long aLong) {

    }

    @Override
    public void remove(String s) {

    }
}
