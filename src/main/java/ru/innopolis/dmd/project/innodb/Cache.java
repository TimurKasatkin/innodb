package ru.innopolis.dmd.project.innodb;

import ru.innopolis.dmd.project.innodb.db.DataManager;
import ru.innopolis.dmd.project.innodb.db.QueryProcessor;
import ru.innopolis.dmd.project.innodb.db.StorageManager;
import ru.innopolis.dmd.project.innodb.db.index.BPlusTreeImpl;
import ru.innopolis.dmd.project.innodb.scheme.Table;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.INDEXES_FILES_DIRECTORY;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.TABLES_FILES_DIRECTORY;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Cache {

    public static QueryProcessor queryProcessor;

    /**
     * Table name -> Table object
     */
    public static Map<String, Table> tables;

    /**
     * Table name -> StorageManager
     */
    public static Map<String, StorageManager> storageManagers;

    static {
        try {
            tables = new HashMap<>();
            File[] tablesFiles = new File(TABLES_FILES_DIRECTORY).listFiles();
            File[] indexesFiles = new File(INDEXES_FILES_DIRECTORY).listFiles();
            List<StorageManager> managers = new LinkedList<>();
            for (File file : tablesFiles) {
                if (file.isFile()) {
                    StorageManager manager = new StorageManager(file);
                    Table table = manager.getTable();
                    List<File> indFiles = Stream.of(indexesFiles)
                            .filter(indFile -> {
                                String indFileName = indFile.getName();
                                return indFileName.startsWith(table.getName())
                                        && indFileName.endsWith(".txt");
                            }).collect(toList());
                    for (File indFile : indFiles) {
                        String name = indFile.getName();
                        String colName = name.substring(table.getName().length()
                                + "_index_".length()).replace(".txt", "");
                        table.addIndex(colName, new BPlusTreeImpl<>(indFile));
                    }
                    managers.add(manager);
                }
            }
            tables = managers.stream()
                    .map(StorageManager::getTable)
                    .collect(toMap(Table::getName, table -> table));
            storageManagers = managers.stream()
                    .collect(toMap(sm -> sm.getTable().getName(), sm -> sm));
            queryProcessor = new QueryProcessor(new DataManager());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public static void main(String[] args) {
        Cache.tables.keySet().forEach(System.out::println);
    }

}
