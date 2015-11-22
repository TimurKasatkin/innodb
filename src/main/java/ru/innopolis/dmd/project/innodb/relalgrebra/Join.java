package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;

import java.util.function.BiPredicate;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Join implements RelationalOperator {

    private final BiPredicate<Row, Row> onPredicate;
    private RelationalOperator operator1, operator2;
    private String tableName1;
    private String tableName2;
    private Table table2;

    public Join(String tableName1, String tableName2, BiPredicate<Row, Row> onPredicate) {
        this(Cache.getTable(tableName1), Cache.getTable(tableName2), onPredicate);
    }

    public Join(Table table1, Table table2, BiPredicate<Row, Row> onPredicate) {
        this(new Scan(table1), table2, onPredicate);
    }

    public Join(RelationalOperator operator1, String tableName2, BiPredicate<Row, Row> onPredicate) {
        this(operator1, Cache.getTable(tableName2), onPredicate);
    }

    public Join(RelationalOperator operator1, Table table2, BiPredicate<Row, Row> onPredicate) {
        this.tableName1 = tableName1;
        this.tableName2 = tableName2;
        this.operator1 = operator1;
        this.table2 = table2;
        this.operator2 = new Scan(this.table2);
        this.onPredicate = onPredicate;
    }


    @Override
    public Row next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return operator1.hasNext() && operator2.hasNext();
    }

    @Override
    public void reset() {
        operator1.reset();
        operator2.reset();
    }
}
