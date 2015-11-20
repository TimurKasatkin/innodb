package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.logic.Condition;
import ru.innopolis.dmd.project.innodb.scheme.Table;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Select implements RelationalOperator {

    private final RelationalOperator operator;
    private final Condition condition;

    public Select(RelationalOperator operator, Condition condition) {
        this.operator = operator;
        this.condition = condition;
    }

    public Select(Table table, Condition condition) {
        this.operator = new Scan(table);
        this.condition = condition;
    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void reset() {
        operator.reset();
    }
}
