package ru.innopolis.dmd.project.innodb.sql;

import ru.innopolis.dmd.project.innodb.scheme.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;

/**
 * @author Timur Kasatkin
 * @date 24.10.15.
 * @email aronwest001@gmail.com
 */
public class Query {

    private QueryType queryType;

    private Table table;

    private MyPredicate predicate;

    /**
     * For inserts and updates
     */
    private Row row;

    /**
     * For updating
     */
    public Query(QueryType queryType, Table table, MyPredicate predicate, Row row) {
        if (queryType == null)
            throw new IllegalArgumentException("Query type should be specified");
        this.queryType = queryType;
        this.table = table;
        this.predicate = predicate == null ? new MyPredicate(null, r -> true) : predicate;
        this.row = row;
    }

    /**
     * For selecting and deleting
     */
    public Query(QueryType queryType, Table table, MyPredicate predicate) {
        this(queryType, table, predicate, null);
    }

    /**
     * For inserts
     */
    public Query(QueryType queryType, Table table, Row row) {
        this(queryType, table, null, row);
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public Table getTable() {
        return table;
    }

    public Row getRow() {
        return row;
    }

    public MyPredicate getPredicate() {
        return predicate;
    }
}
