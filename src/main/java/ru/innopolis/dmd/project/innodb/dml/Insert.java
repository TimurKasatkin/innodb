package ru.innopolis.dmd.project.innodb.dml;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.entry;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.map;
import static ru.innopolis.dmd.project.innodb.utils.RowUtils.pkValue;

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
        if (!table.test(row))
            throw new IllegalArgumentException("Row violates table constraint");
        this.row = row;
        this.table = table;
    }

    public static void main(String[] args) {
//        for (int i = 5000; i < 10000; i++) {
//            new Insert(new Row(map(
//                    entry("id", i),
//                    entry("title", "title #" + i),
//                    entry("publtype", "journal_article"),
//                    entry("url", "url #" + i),
//                    entry("year", null)
//            )), "articles").execute();
//        }
//        for (int i = 4000; i < 10000; i++) {
//            new Insert(new Row(map(
//                    entry("id", i),
//                    entry("name", "conference #" + i)
//            )), "conferences").execute();
//        }
        for (int i = 40000; i < 100000; i++) {
            new Insert(
                    new Row(map(
                            entry("article_id", i),
                            entry("conference_id", i)
                    )), "article_conference"
            ).execute();
        }
    }

    @Override
    public void execute() {
        table.getPkIndex().insert(pkValue(row, table), row);
    }


}
