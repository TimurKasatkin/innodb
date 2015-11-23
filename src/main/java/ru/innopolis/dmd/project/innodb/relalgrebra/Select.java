package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.logic.Condition;
import ru.innopolis.dmd.project.innodb.logic.ConditionType;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;

import java.util.List;
import java.util.stream.Collectors;

import static ru.innopolis.dmd.project.innodb.logic.ConditionType.EQUALS;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.stream;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Select implements RelationalOperator {

    private RelationalOperator operator;

    private Condition condition;

    private int offset = -1;
    private int limit = -1;
    private int pageNumber;

    private boolean foundByPk = false;

    private boolean hasIndex = false;

    private boolean uniqueIndex = false;

    private Table table;

    private Row currentRow;
    private String tableName;

    private int rowCount = 0;

//    public Select(String tableName, Condition condition, int limit, int offset) {
//        this(Cache.getTable(tableName), condition, limit, offset);
//    }

//    public Select(Table table, Condition condition, int limit, int offset) {
//        if (offset < 0)
//            throw new IllegalArgumentException("Offset should not be negative");
//        if (limit <= 0)
//            throw new IllegalArgumentException("Offset should not be negative or zero");
//        this.limit = limit;
//        this.offset = offset;
//        this.pageNumber = table.getPageNumber() + 1;
//        this.table = table;
//        this.tableName = table.getName();
//        this.condition = condition;
//        List<Condition> conditions = condition.asList();
//        List<ConditionType> conditionTypes = condition.conditionTypes();
//        List<Column> columns = stream(conditions)
//                .map(Condition::getColumnName)
//                .map(table::getColumn)
//                .collect(Collectors.toList());
//        List<Column> primaryKeys = table.getPrimaryKeys();
//        if (primaryKeys.equals(columns) && stream(conditionTypes)
//                .allMatch(condType -> condType.equals(EQUALS))) {
//            String pkValue = "";
//            for (Column primaryKey : primaryKeys) {
//                Condition pkCondition = stream(conditions)
//                        .filter(cond -> cond.getColumnName().equals(primaryKey.getName()))
//                        .findFirst().orElse(null);
//                pkValue += pkCondition.getParams().get(0);
//            }
//            currentRow = table.getPkIndex().search(pkValue);
//            if (currentRow != null) {
//                rowCount = 1;
//            }
//            if (offset > 0) {
//                currentRow = null;
//                rowCount = 0;
//            }
//            foundByPk = true;
//        } else {
//            this.operator = new Scan(table);
//            while (operator.hasNext()) {
//                Row next = operator.next();
//                if (condition.test(next)) {
//                    currentRow = next;
//                    break;
//                }
//            }
//        }
//    }

    public Select(String tableName, Condition condition) {
        this(Cache.getTable(tableName), condition);
    }

    public Select(Table table, Condition condition) {
        this.pageNumber = table.getPageNumber() + 1;
        this.table = table;
        tableName = table.getName();
        this.condition = condition;
        List<Condition> conditions = condition.asList();
        List<ConditionType> conditionTypes = condition.conditionTypes();
        List<Column> columns = stream(conditions)
                .map(Condition::getColumnName)
                .map(table::getColumn)
                .collect(Collectors.toList());
        List<Column> primaryKeys = table.getPrimaryKeys();
        if (primaryKeys.equals(columns) && stream(conditionTypes)
                .allMatch(condType -> condType.equals(EQUALS))) {
            String pkValue = "";
            for (Column primaryKey : primaryKeys) {
                Condition pkCondition = stream(conditions)
                        .filter(cond -> cond.getColumnName().equals(primaryKey.getName()))
                        .findFirst().orElse(null);
                pkValue += pkCondition.getParams().get(0);
            }
            currentRow = table.getPkIndex().search(pkValue);
            foundByPk = true;
        } else {
            this.operator = new Scan(table);
            while (operator.hasNext()) {
                Row next = operator.next();
                if (next != null && condition.test(next)) {
                    currentRow = next;
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
//        Select select = new Select("articles", new Condition("publtype", EQUALS, list("journal_article"),
//                LogicalOperation.OR, new Condition("publtype", EQUALS, "conference_article")));
//        List<Row> rows = select.loadAll();
//        System.out.println(rows.size());
//        System.out.println(new Scan("articles").loadAll().size());
//        Select select = new Select("articles", new Condition("title", ConditionType.LIKE_INSENSITIVE, "learning"));
        Select select = new Select("users", new Condition("login", EQUALS, "user"));
//        List<Row> rows = select.loadAll();
//        rows.forEach(System.out::println);
        while (select.hasNext())
            System.out.println(select.next());
    }

    @Override
    public Row next() {
        if (foundByPk && currentRow != null) {
            Row currentRow = this.currentRow;
            this.currentRow = null;
            return currentRow;
        }
        if (currentRow != null) {
            Row currentRow = this.currentRow;
            this.currentRow = null;
            return currentRow;
        }
        while (operator.hasNext()) {
            Row next = operator.next();
            if (next != null && condition.test(next)) {
                currentRow = next;
                return next;
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (foundByPk && currentRow != null)
            return true;
        if (currentRow != null) {
            return true;
        }
        while (operator.hasNext()) {
            Row next = operator.next();
            if (next != null && condition.test(next)) {
                currentRow = next;
                return true;
            }
        }
        return false;
    }

    @Override
    public void reset() {
        operator.reset();
        currentRow = null;
        rowCount = 0;
    }

    private boolean hasOffset() {
        return offset != -1;
    }

    private boolean hasLimit() {
        return limit != -1;
    }
}
