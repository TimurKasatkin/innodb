package ru.innopolis.dmd.project.innodb.operations;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class Insert implements Executable {

    private Row row;

    private Table table;

    public Insert(Row row, String tableName) {
        this(row, Cache.getTable(tableName));
    }

    public Insert(Row row, Table table) {
        if (table == null)
            throw new IllegalArgumentException("There is no table");
        table.test(row);
        this.row = row;
        this.table = table;
    }

    @Override
    public void execute() {

    }
}
