package ru.innopolis.dmd.project.innodb.utils;

import ru.innopolis.dmd.project.innodb.db.PageType;
import ru.innopolis.dmd.project.innodb.db.page.Page;
import ru.innopolis.dmd.project.innodb.db.page.TableDataPage;

import java.io.IOException;
import java.io.RandomAccessFile;

import static ru.innopolis.dmd.project.innodb.db.PageType.*;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.*;

/**
 * @author Timur Kasatkin
 * @date 21.11.15.
 * @email aronwest001@gmail.com
 */
public class PageUtils {

    public static void assertPageTypesEquals(RandomAccessFile raf, int pageNum, PageType pageType) {
        try {
            setToPage(raf, pageNum);
            PageType type = PageType.byMarker((char) raf.readByte());
            if (!type.equals(pageType))
                throw new IllegalStateException("Expected: " + pageType + "; Actual type of page " + pageNum + ": " + type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Page getPage(int pageNumber) {
        RandomAccessFile raf = createRaf("r");
        Page page = getPage(pageNumber, raf);
        closeRaf(raf);
        return page;
    }


    public static Page getPage(int pageNumber, RandomAccessFile raf) {
        try {
            setToPage(raf, pageNumber);
            String rawData = raf.readLine();
            PageType pageType = PageType.byMarker(
//                    ((char) raf.readByte())
                    rawData.charAt(0)
            );
            switch (pageType) {
                case FREE_PAGE:
                    return new Page(pageNumber, FREE_PAGE, rawData);
                case DATABASE_META:
                    return new Page(pageNumber, DATABASE_META, rawData);
                case DATABASE_SCHEME:
                    return new Page(pageNumber, DATABASE_SCHEME, rawData);
                case TABLE_SCHEME:
                    return new Page(pageNumber, TABLE_SCHEME, rawData);
                case TABLE_DATA:
                    return new TableDataPage(pageNumber, rawData);
                case INDEX_SCHEME:
                    return new Page(pageNumber, INDEX_SCHEME, rawData);
                case INDEX_DATA:
                    return new Page(pageNumber, INDEX_DATA, rawData);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) {
        Page page = getPage(7);
        page.deserialize();
        System.out.println();
    }

}