package ru.innopolis.dmd.project.innodb.utils;

import ru.innopolis.dmd.project.innodb.db.DBConstants;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class StorageUtils {

    /**
     * Place specified rag at page with given number and returns offset to page in bytes
     */
    public static long setToPage(RandomAccessFile raf, int pageNum) throws IOException {
        long offset = pageNum * DBConstants.PAGE_LENGTH;
        raf.seek(offset);
        return offset;
    }

    public static String getRow(RandomAccessFile raf, long offset) throws IOException {
        raf.seek(offset);
        char c = (char) raf.readByte();
        if (c != '[') throw new IllegalArgumentException("There is no row starts at specified position");
        StringBuilder builder = new StringBuilder();
        builder.append(c);
        while (c != ']') {
            c = (char) raf.readByte();
            builder.append(c);
        }
        return builder.toString();
    }

    public String readChars(RandomAccessFile raf, int count) throws IOException {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < count; i++)
            result.append((char) raf.readByte());
        return result.toString();
    }

    public String readChars(RandomAccessFile raf, int count, long offset) throws IOException {
        raf.seek(offset);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < count; i++)
            result.append((char) raf.readByte());
        return result.toString();
    }


}
