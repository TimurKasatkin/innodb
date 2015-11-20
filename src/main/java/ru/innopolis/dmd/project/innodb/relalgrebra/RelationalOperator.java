package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public interface RelationalOperator {

    Row next();

    boolean hasNext();

    void reset();

    default List<Row> loadAll() {
        List<Row> rows = new LinkedList<>();
        while (hasNext())
            rows.add(next());
        return rows;
    }

}
