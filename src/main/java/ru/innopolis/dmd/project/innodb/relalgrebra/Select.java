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

    public Select(RelationalOperator operator, Condition condition) {
        this.operator = operator;
        this.condition = condition;

    }

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
//        List<String> columnNames = condition.getColumnNames();
        List<Column> columns = stream(conditions)
                .map(Condition::getColumnName)
                .map(table::getColumn)
                .collect(Collectors.toList());
        List<Column> primaryKeys = table.getPrimaryKeys();
        if (primaryKeys.equals(columns) && stream(conditionTypes).allMatch(condType -> condType.equals(EQUALS))) {
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
        }
    }

    public static void main(String[] args) {
        Select select = new Select("articles", new Condition("id", EQUALS, 180000));
        System.out.println(select.next());
    }

    @Override
    public Row next() {
        if (foundByPk && currentRow != null) {
            Row currentRow = this.currentRow;
            this.currentRow = null;
            return currentRow;
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (foundByPk && currentRow != null)
            return true;

        return false;
    }

    @Override
    public void reset() {
        operator.reset();
    }
}
