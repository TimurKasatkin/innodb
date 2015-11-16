package ru.innopolis.dmd.project.innodb.db;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.sql.*;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static ru.innopolis.dmd.project.innodb.sql.SQLCOnstants.*;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class QueryProcessor {

    private static final String fromWord = "FROM";
    private static final String whereWord = "WHERE";
    private static final String intoWord = "INTO";
    private static final String valuesWord = "VALUES";
    private static final String updateWord = "UPDATE";
    private static final String setWord = "SET";

    private DataManager dataManager;

    public QueryProcessor(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public static void main(String[] args) {
//        Query query = QueryProcessor.parseQuery("INSERT INTO Employee (id, name, designation, Address) " +
//                "VALUES (228,'name 228','designation 228','Address 228')");
//
//        query = QueryProcessor.parseQuery("SELECT * FROM Employee WHERE id>=15 AND name='name 15'");
//
//        query = QueryProcessor.parseQuery("SELECT * FROM Employee");
//
//        query = QueryProcessor.parseQuery("DELETE FROM Student WHERE id<19 OR Address='Addr 19'");
//
//        query = QueryProcessor.parseQuery("delete from Student");
//
//        query = QueryProcessor.parseQuery("update Employee set id=5, name='name 6' WHERE id<=5 OR designation='design 5'");

        QueryProcessor queryProcessor = Cache.queryProcessor;
        for (int i = 1; i < 10000; i++) {
            queryProcessor.execute(MessageFormat.format("insert into Student (id,name,email,address) " +
                    "values({0},''{1}'',''{2}'',''{3}'')", i, "name " + i, "email " + i, "addr " + i));
        }
//        for (int i = 1; i < 100; i++) {
//            queryProcessor.execute(MessageFormat.format("insert into Employee (id,name,designation,Address) " +
//                    "values({0},''{1}'',''{2}'',''{3}'')", i, "name " + i, "designation " + i, "addr " + i));
//        }
//        System.out.println(queryProcessor.execute("SELECT * FROM Employee"));
//        System.out.println(queryProcessor.execute("SELECT * FROM Employee WHERE name='name 5'"));
//
//        queryProcessor.execute("UPDATE Employee SET name='name Hz'");
//        queryProcessor.execute("UPDATE Employee SET name='name H' WHERE name='name 15'");
//
//        queryProcessor.execute("DELETE FROM Employee WHERE id=10");
//        queryProcessor.execute("DELETE FROM Employee");

    }

    public static Query parseQuery(String queryStr) {
        String[] tokens = queryStr.split(" ");
        switch (tokens[0].toLowerCase()) {
            case "insert":
                return parseInsert(queryStr);
            case "select":
                return parseSelect(queryStr);
            case "update":
                return parseUpdate(queryStr);
            case "delete":
                return parseDelete(queryStr);
            default:
                throw new IllegalArgumentException("Invalid SQL");
        }
    }

    //INSERT INTO tablename (field1,field2,...) VALUES (?,?,...)
    private static Query parseInsert(String query) {
        int intoInd = indexOfWord(intoWord, query);
        int valuesInd = indexOfWord(valuesWord, query);
        if (intoInd == -1 || valuesInd == -1)
            throw new IllegalArgumentException("Invalid SQL");
        String tableName = query.substring(intoInd + intoWord.length() + 1,
                valuesInd).trim().split(" ")[0].trim();
        Table table = tryGetTable(tableName);

        List<Column> columns = table.getColumns();
        Set<String> columnNames = columns.stream().map(Column::getName).collect(toSet());

        //Get declared fields
        String fieldsStr = query
                .substring(query.indexOf(tableName) + tableName.length(), valuesInd).trim();
        fieldsStr = fieldsStr.substring(1, fieldsStr.length() - 1);
        List<String> fields = Stream.of(fieldsStr.split(","))
                .map(String::trim).collect(toList());

        if (fields.stream().anyMatch(fieldName -> !columnNames.contains(fieldName)))
            throw new IllegalArgumentException("Invalid set of fields");
        String valuesStr = query.substring(valuesInd + valuesWord.length(), query.length()).trim();
        //Remove brackets
        valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
        List<Comparable> values = new LinkedList<>();
        Matcher matcher = SQL_INSERT_VALUES_REGEXP.matcher(valuesStr);
        int i = 0;
        while (matcher.find()) {
            String valueStr = matcher.group(1).trim();
            if (valueStr.startsWith("'"))
                valueStr = valueStr.substring(1);
            if (valueStr.endsWith("'"))
                valueStr = valueStr.substring(0, valueStr.length() - 1);
            values.add(table.getColumn(fields.get(i)).getType().parse(valueStr));
            i++;
        }
        if (columns.size() != values.size())
            throw new IllegalArgumentException("Invalid SQL: fields count " +
                    "and values count are not equals");
        Map<String, Comparable> entries = new HashMap<>();
        for (i = 0; i < columns.size(); i++) {
            entries.put(columns.get(i).getName(), values.get(i));
        }
        return new Query(QueryType.INSERT, table, new Row(entries));
    }

    //SELECT * FROM tablename (WHERE field1=? AND field2=? OR field2=?)?
    //TODO it does not deal with brackets, NOT, fields each to other comparison
    private static Query parseSelect(String query) {
        int fromInd = indexOfWord(fromWord, query);
        if (fromInd == -1) throw new IllegalArgumentException("Invalid SQL");
        int whereInd = indexOfWord(whereWord, query);
        String tableName = query.substring(fromInd + fromWord.length(),
                whereInd != -1 ? whereInd : query.length()).trim();
        Table table = tryGetTable(tableName);
        return new Query(QueryType.SELECT, table, parsePredicate(query, table));
    }

    //UPDATE tablename SET field=? WHERE any_field=?
    private static Query parseUpdate(String query) {
        int updateInd = indexOfWord(updateWord, query);
        int setInd = indexOfWord(setWord, query);
        int whereInd = indexOfWord(whereWord, query);
        String tableName = query.substring(updateInd + updateWord.length(), setInd).trim();
        Table table = tryGetTable(tableName);
        String[] setExpressions = query.substring(setInd + setWord.length(),
                whereInd != -1 ? whereInd : query.length()).trim().split(",");
        Map<String, Comparable> entries = new HashMap<>();
        for (String setExpression : setExpressions) {
            String[] split = setExpression.split("=");
            String columnName = split[0].trim();
            Column column = table.getColumn(columnName);
            if (column == null)
                throw new IllegalArgumentException("Invalid SQL: there is no field '" + columnName + "'");
            String valueStr = split[1].trim();
            if (valueStr.startsWith("'"))
                valueStr = valueStr.substring(1);
            if (valueStr.endsWith("'"))
                valueStr = valueStr.substring(0, valueStr.length() - 1);
            entries.put(columnName, column.getType().parse(valueStr));
        }
        return new Query(QueryType.UPDATE, table, parsePredicate(query, table), new Row(entries));
    }

    //DELETE FROM tablename WHERE field=?
    private static Query parseDelete(String query) {
        int fromInd = indexOfWord(fromWord, query);
        int whereInd = indexOfWord(whereWord, query);
        String tableName = query.substring(fromInd + fromWord.length(),
                whereInd != -1 ? whereInd : query.length()).trim();
        Table table = tryGetTable(tableName);
        return new Query(QueryType.DELETE, table, parsePredicate(query, table));
    }

    private static MyPredicate parsePredicate(String query, Table table) {
        int whereInd = indexOfWord(whereWord, query);
        if (whereInd != -1) {
            Scanner conditionSc = new Scanner(query.substring(whereInd + whereWord.length()));
            String lastLogicalOperand = "";
            List<Condition> conditions = new LinkedList<>();
            Predicate<Row> predicate = null;
            while (conditionSc.hasNext()) {
                conditionSc.useDelimiter(SQL_COMPARISONS_REGEXP);
                String fieldName = conditionSc.next().trim();
                Column column = table.getColumn(fieldName);
                if (column == null)
                    throw new IllegalArgumentException("Invalid field name");
                String operator = conditionSc.findInLine(SQL_COMPARISONS_REGEXP).trim();
                ConditionType conditionType = ConditionType.byOperator(operator);
                Comparable[] args = new Comparable[conditionType.getNumOfArgs()];
                conditionSc.useDelimiter(SQL_LOGICAL_OPERANDS_REGEXP);
                for (int i = 0; i < args.length; i++) {
                    String valueStr = conditionSc.next().trim();
                    if (valueStr.startsWith("'"))
                        valueStr = valueStr.substring(1);
                    if (valueStr.endsWith("'"))
                        valueStr = valueStr.substring(0, valueStr.length() - 1);
                    args[i] = column.getType().parse(valueStr.replaceAll("[,()]", ""));
                }
                Condition condition = new Condition(fieldName, conditionType, args);
                conditions.add(condition);
                if (predicate == null)
                    predicate = condition::matches;
                else {
                    switch (lastLogicalOperand.toLowerCase().trim()) {
                        case "and":
                            predicate = predicate.and(condition::matches);
                            break;
                        case "or":
                            predicate = predicate.or(condition::matches);
                            break;
                    }
                }
                lastLogicalOperand = conditionSc.findInLine(SQL_LOGICAL_OPERANDS_REGEXP);
            }
            return new MyPredicate(conditions, predicate);
        }
        return new MyPredicate(null, row -> true);
    }

    private static Table tryGetTable(String tableName) {
        Table table = Cache.getTable(tableName);
        if (table == null)
            throw new IllegalArgumentException("There is no table '" + tableName + "'");
        return table;
    }

    private static int indexOfWord(String word, String str) {
        int ind = str.indexOf(word);
        if (ind == -1) ind = str.indexOf(word.toLowerCase());
        return ind;
    }

    //UPDATE tablename SET field=? WHERE any_field=?
//    public void update(String query) {
//        int updateInd = query.indexOf(updateWord);
//        int setInd = query.indexOf(setWord);
//        int whereInd = query.indexOf(whereWord);
//        String tableName = query.substring(updateInd + updateWord.length(), setInd).trim();
//        String[] split = query.substring(setInd + setWord.length(),
//                whereInd != -1 ? whereInd : query.length()).trim().split("=");
//        Entry<String, Object> updateValue = new SimpleEntry<>(split[0], split[1]);
//        Entry<String, Object> equality = null;
//        if (whereInd != -1) {
//            split = query.substring(whereInd + whereWord.length(), query.length()).trim().split("=");
//            equality = new SimpleEntry<>(split[0], split[1]);
//        }
//        dataManager.update(tableName, updateValue, equality);
//    }

    public Collection<Row> execute(String query) {
        return dataManager.execute(parseQuery(query));
//        String command = query.split(" ")[0];
//        switch (command) {
//            case "INSERT":
//                insert(query);
//                return null;
//            case "SELECT":
//                return select(query);
//            case "UPDATE":
//                update(query);
//                return null;
//            case "DELETE":
//                delete(query);
//                return null;
//            default:
//                throw new IllegalArgumentException("Invalid SQL");
//        }
    }

    //SELECT * FROM tablename (WHERE any_field=?)?
//    public Collection<Row> select(String query) {
//        int fromInd = query.indexOf(fromWord);
//        int whereInd = query.indexOf(whereWord);
//        String tableName = query.substring(fromInd + fromWord.length(),
//                whereInd != -1 ? whereInd : query.length()).trim();
//        Entry<String, Object> equality = null;
//        if (whereInd != -1) {
//            String[] whereSplit = query.substring(whereInd + 6).split("=");
//            equality = new SimpleEntry<>(whereSplit[0], whereSplit[1].replace("\'", ""));
//        }
//        return dataManager.select(tableName, equality);
//    }

    //INSERT INTO tablename (field1,field2,...) VALUES (?,?,...)
//    public void insert(String query) {
//        int intoInd = query.indexOf(intoWord);
//        int valuesInd = query.indexOf(valuesWord);
//        String tableName = query.substring(intoInd + intoWord.length() + 1, valuesInd)
//                .trim().split(" ")[0].trim();
//        String fieldsStr = query
//                .substring(query.indexOf(tableName) + tableName.length(), valuesInd).trim();
//        fieldsStr = fieldsStr.substring(1, fieldsStr.length() - 1);
//        List<String> fields = Stream.of(fieldsStr.split(","))
//                .map(String::trim).collect(toList());
//        String valuesStr = query
//                .substring(valuesInd + valuesWord.length(), query.length()).trim();
//        valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
//        List<String> values = new LinkedList<>();
//        Matcher matcher = SQL_INSERT_VALUES_REGEXP.matcher(valuesStr);
//        while (matcher.find())
//            values.add(matcher.group(1).trim().replace("'", ""));
//        if (fields.size() != values.size())
//            throw new IllegalArgumentException("Invalid SQL: fields count " +
//                    "and values count are not equals");
//        Map<String, Comparable> entries = new HashMap<>();
//        for (int i = 0; i < fields.size(); i++)
//            entries.put(fields.get(i), values.get(i));
//        dataManager.insert(tableName, new Row(entries));
//    }

    //DELETE FROM tablename WHERE field=?
//    public void delete(String query) {
//        int fromInd = query.indexOf(fromWord);
//        int whereInd = query.indexOf(whereWord);
//        String tableName = query.substring(fromInd + fromWord.length(),
//                whereInd != -1 ? whereInd : query.length()).trim();
//        Entry<String, Object> equality = null;
//        if (whereInd != -1) {
//            String[] split = query.substring(whereInd + whereWord.length()).split("=");
//            equality = new SimpleEntry<>(split[0], split[1].replace("'", ""));
//        }
//        dataManager.delete(tableName, equality);
//    }

}
