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

import static ru.innopolis.dmd.project.innodb.db.DBConstants.DB_FILE;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.DB_SCHEME_PATTERN;
import static ru.innopolis.dmd.project.innodb.db.PageType.*;
import static ru.innopolis.dmd.project.innodb.utils.StorageUtils.setToPage;

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
            setToPage(dbRaf, 0);
            char marker = (char) dbRaf.readByte();
            if (!PageType.byMarker(marker).equals(DATABASE_META))
                throw new IllegalStateException("There is no database meta page");

            setToPage(dbRaf, 1);
            marker = (char) dbRaf.readByte();
            if (!PageType.byMarker(marker).equals(DATABASE_SCHEME))
                throw new IllegalStateException("There is no database scheme page");
            String dbScheme = dbRaf.readLine();
            Matcher matcher = DB_SCHEME_PATTERN.matcher(dbScheme);
            while (matcher.find()) {
                String tableName = matcher.group("tablename");
                int pageNumber = Integer.parseInt(matcher.group("pagenumber"));
                setToPage(dbRaf, pageNumber);
                if (!PageType.byMarker((char) dbRaf.readByte()).equals(TABLE_SCHEME))
                    throw new IllegalStateException("There is no table scheme page");
                String tableScheme = dbRaf.readLine();
                Table table = SchemeUtils.parseTable(tableScheme);
                table.setPageNumber(pageNumber);
                tables.put(table.getName(), table);
            }


//            File[] tablesFiles = new File(TABLES_FILES_DIRECTORY).listFiles();
//            File[] indexesFiles = new File(INDEXES_FILES_DIRECTORY).listFiles();
//            List<StorageManager> managers = new LinkedList<>();
//            for (File file : tablesFiles) {
//                if (file.isFile()) {
//                    StorageManager manager = new StorageManager(file);
//                    Table table = manager.getTable();
//                    List<File> indFiles = Stream.of(indexesFiles)
//                            .filter(indFile -> {
//                                String indFileName = indFile.getName();
//                                return indFileName.startsWith(table.getName())
//                                        && indFileName.endsWith(".txt");
//                            }).collect(toList());
//                    for (File indFile : indFiles) {
//                        String name = indFile.getName();
//                        String colName = name.substring(table.getName().length()
//                                + "_index_".length()).replace(".txt", "");
//                        table.addIndex(colName, new MultivaluedDBMapBPlusTreeIndexImpl<>(indFile));
//                    }
//                    managers.add(manager);
//                }
//            }
//            tables = managers.stream()
//                    .map(StorageManager::getTable)
//                    .collect(toMap(Table::getName, table -> table));
//            storageManagers = managers.stream()
//                    .collect(toMap(sm -> sm.getTable().getName(), sm -> sm));
//            dataManager = new DataManager();
//            dbRaf = new RandomAccessFile(new File(DATABASE_FILE_PATH),"rw");
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
