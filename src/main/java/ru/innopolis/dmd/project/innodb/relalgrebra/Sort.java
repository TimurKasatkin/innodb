package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Sort implements RelationalOperator {

    private final String sortBy;
    private final RelationalOperator operator;

    private int limit, offset;

    public Sort(String sortBy, RelationalOperator operator) {
        this.sortBy = sortBy;
        this.operator = operator;
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

    }
}
