package ru.innopolis.dmd.project.innodb;

import ru.innopolis.dmd.project.innodb.db.DataManager;
import ru.innopolis.dmd.project.innodb.db.PageType;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.utils.SchemeUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;
import static ru.innopolis.dmd.project.innodb.db.PageType.*;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.readChars;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.setToPage;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.assertPageTypesEquals;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Cache implements AutoCloseable {

    public static DataManager dataManager;

    /**
     * Table name -> Table object
     */
    public static Map<String, Table> tables = new HashMap<>();

    public static RandomAccessFile dbRaf;

    public static int pagesCount = 0;

    public static int firstFreePage = -1;

    /**
     * Table name -> StorageManager
     */
//    public static Map<String, StorageManager> storageManagers;

    static {
        try {
            dbRaf = new RandomAccessFile(DB_FILE, "rw");
            assertPageTypesEquals(dbRaf, 0, DATABASE_META);
            pagesCount = Integer.parseInt(readChars(dbRaf, PAGES_COUNT_LENGTH).replaceAll("\\[|]|_", ""));
            assertPageTypesEquals(dbRaf, 1, DATABASE_SCHEME);
            String dbScheme = dbRaf.readLine();
            Matcher matcher = DB_SCHEME_PATTERN.matcher(dbScheme);
            while (matcher.find()) {
                //String tableName = matcher.group("tablename");
                int pageNumber = Integer.parseInt(matcher.group("pagenumber"));
                setToPage(dbRaf, pageNumber);
                if (!PageType.byMarker((char) dbRaf.readByte()).equals(TABLE_SCHEME))
                    throw new IllegalStateException("There is no table scheme page");
                String tableScheme = dbRaf.readLine();
                Table table = SchemeUtils.parseTable(pageNumber, tableScheme);
                tables.put(table.getName(), table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public static void main(String[] args) {
        int pagesCount = Cache.pagesCount;
//        Cache.tables.keySet().forEach(System.out::println);
    }


    @Override
    public void close() throws Exception {
        if (dbRaf != null)
            dbRaf.close();
    }
}
