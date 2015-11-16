package ru.innopolis.dmd.project.innodb.db;

import ru.innopolis.dmd.project.innodb.db.index.BPlusTree;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Row;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.sql.Condition;
import ru.innopolis.dmd.project.innodb.sql.ConditionType;
import ru.innopolis.dmd.project.innodb.sql.MyPredicate;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;
import static ru.innopolis.dmd.project.innodb.sql.ConditionType.*;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class StorageManager implements AutoCloseable {

    private Table table;
    private RandomAccessFile raf;
    /**
     * in bytes!!!
     */
    private int schemaLineLength;

    private File file;

    private int pageCount = PAGES_COUNT;

    public StorageManager(File file) throws IOException {
        if (!file.exists())
            throw new IllegalArgumentException("File does not exists");
        if (file.isDirectory())
            throw new IllegalArgumentException("Directory passed");
        this.file = file;
        this.raf = new RandomAccessFile(file, "rwd");
        raf.seek(0);
        String scheme = raf.readLine();
        this.schemaLineLength = scheme.length() + 1;
        this.table = Table.parseTable(file.getName().replace(".txt", ""), scheme);
    }

    //RS hash
    static int hash(String str) {
        int hash = 0, i = 5, j = 3;
        for (char c : str.toCharArray()) {
            hash = (hash * i) + c;
            i *= j;
        }
        return hash;
    }

    //74    [5$name 5$designation 5$address 5][15$name 15$designation 15$address 15]
    //SELECT * FROM Employee
    //INSERT INTO Employee (id,name,designation,Address) VALUES (16,'name 16','designation 16','Address 16')
    public void insert(Row row) {
        List<String> colNames = table.getColumns()
                .stream().map(Column::getName).collect(toList());
        //TODO auto id calculation (if it is possible)
        if (!row.getColumnNames().stream().allMatch(colNames::contains))
            throw new IllegalArgumentException("Row contains not all columns");
        try {
            List<String> pkNames = table.getPrimaryKeys()
                    .stream().map(Column::getName).collect(toList());
            String pkValuesConcatenation = String.join("",
                    pkNames.stream()
                            .map(row::getValue)
                            .map(Object::toString)
                            .collect(toList()));
            int ind = hash(pkValuesConcatenation) % PAGES_COUNT;
            String formattedRow = format(row);
            long offset = goToPage(ind);

            while (true) {
                String posToJumpStr = "";
                long currentPosition = raf.getFilePointer();
                for (int i = 0; i < CURSOR_TO_ADD_NUM_LENGTH; i++)
                    posToJumpStr += (char) raf.readByte();
                posToJumpStr = posToJumpStr.trim();
                if (posToJumpStr.isEmpty()) posToJumpStr = "0";
                int byteToJump = Integer.parseInt(posToJumpStr.trim());
                if (byteToJump + formattedRow.length() > PAGE_LENGTH) {
                    String jumpToPage = "" + (char) raf.readByte() + (char) raf.readByte();
                    offset = jumpToPage.trim().length() == 0 ?
                            createNewPage(currentPosition) :
                            jumpToNextPage(Integer.parseInt(jumpToPage.trim()));
                } else {
                    raf.skipBytes(NEXT_PAGE_JUMP_NUM_LENGTH + byteToJump);
                    long posToTree = raf.getFilePointer();
                    raf.writeBytes(formattedRow);
                    raf.seek(offset);
                    raf.writeBytes("    ");
                    raf.seek(raf.getFilePointer() - CURSOR_TO_ADD_NUM_LENGTH);
                    raf.writeBytes((byteToJump + formattedRow.length()) + "");
                    Map<String, BPlusTree<String, Long>> indexes = table.getIndexes();
//                    List<Map.Entry<String, BPlusTree<String, Long>>> indexes = indexes.entrySet().stream()
//                            .filter(entry -> row.getColumnNames().contains(entry.getKey()))
//                            .collect(toList());
//                    indexes.forEach((key, value) ->
//                            value.insert(row.getValue(key).toString(), posToTree));
//                    table.getIndexes().keySet().forEach(index -> table
//                            .getIndexes().get(index)
//                            .insert(row.getValue(index).toString(), posToTree));
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Table getTable() {
        return table;
    }

    public Collection<Row> select(MyPredicate predicate) {
        if (predicate.getColumnNames().stream()
                .anyMatch(table.getIndexes()::containsKey)) {
            return selectByTree(predicate);
        }
        if (table.getPrimaryKeys().stream()
                .map(Column::getName)
                .allMatch(predicate.getColumnNames()::contains)) {
            return selectById(predicate);
        }
        Collection<Row> rows = new LinkedList<>();
        try {
            goToPage(0);
            while (raf.getFilePointer() < raf.length()) {
                String page = raf.readLine();
                rows.addAll(Row.parseRows(table.getColumns(), page)
                        .stream()
                        .filter(predicate)
                        .collect(toList()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rows;
    }

    public void update(
            //Contains Field->New value
            //for all fields, which we want to update
            Row row, MyPredicate predicate) {

        Collection<Row> delRows = delete(predicate);
        delRows.forEach(r -> insert(upd(r, row)));
    }

    //TODO fix
    public Collection<Row> delete(MyPredicate predicate) {
        Collection<Row> delRows = new LinkedList<>();
        try {
            goToPage(0);
            while (raf.getFilePointer() < raf.length()) {
                char tmp = (char) raf.readByte();
                if (tmp == '[') {
                    long position = raf.getFilePointer() - 1;
                    Row row = getRow(position);
                    if (predicate.test(row)) {
                        delete(position, row.toString().length());
                        table.getIndexes().entrySet()
                                .forEach(entry -> entry.getValue()
                                        .remove(row.getValue(entry.getKey()).toString(), position));
                        delRows.add(row);
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return delRows;
    }

    private long createNewPage(long lastPageStart) throws IOException {
        long pos = file.length();
        raf.seek(pos);
        StringBuilder sb = new StringBuilder();
        sb.append("0");
        for (int i = 0; i < PAGE_LENGTH + CURSOR_TO_ADD_NUM_LENGTH + NEXT_PAGE_JUMP_NUM_LENGTH - 1; i++) {
            sb.append(" ");
        }
        sb.append('\n');
        raf.writeBytes(sb.toString());
        long jump = (pos - lastPageStart) / (PAGE_LENGTH + CURSOR_TO_ADD_NUM_LENGTH + NEXT_PAGE_JUMP_NUM_LENGTH);
        raf.seek(lastPageStart + 4);
        raf.writeBytes(String.valueOf(jump));
        raf.seek(pos);
        return pos;
    }

    private long jumpToNextPage(int jump) throws IOException {
        raf.seek(raf.getFilePointer()
                + ((PAGE_LENGTH + CURSOR_TO_ADD_NUM_LENGTH
                + NEXT_PAGE_JUMP_NUM_LENGTH) * jump) - 5);
        return raf.getFilePointer();
    }

    private String format(Row row) {
        return "[" + table.getColumns().stream()
                .map(Column::getName)
                .map(row::getValue)
                .map(Object::toString)
                .collect(Collectors.joining("$")) + "]";
    }

    /**
     * Go to page with specified number
     * and return number of bytes before the page starting from beginning of the file.
     *
     * @param num number of page
     * @return number of bytes before the page starting from beginning of the file.
     * @throws IOException
     */
    private long goToPage(int num) throws IOException {
        long offset = schemaLineLength + num * (CURSOR_TO_ADD_NUM_LENGTH
                + NEXT_PAGE_JUMP_NUM_LENGTH
                + PAGE_LENGTH + /*'\n' character length*/1);
        raf.seek(offset);
        return offset;
    }

    @Override
    public void close() {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Collection<Row> selectByTree(MyPredicate predicate) {
        Collection<Row> rows = new LinkedList<>();
        List<String> colNames = predicate.getColumnNames();
        List<Condition> conditions = predicate.getConditions();
        for (int i = 0; i < colNames.size(); i++) {
            if (table.getIndexes().containsKey(colNames.get(i))) {
                BPlusTree<String, Long> tree = table.getIndexes().get(colNames.get(i));
                Condition condition = conditions.get(i);
                ConditionType condType = condition.getConditionType();
                List<String> condParamsStrs = condition.getParams().stream()
                        .map(Object::toString)
                        .collect(toList());
                List<Long> indexes = new LinkedList<>();
                if (condType.equals(EQUALS) | condType.equals(MORE_OR_EQUALS)
                        | condType.equals(LESS_OR_EQUALS))
                    indexes.addAll(tree.search(condParamsStrs.get(0)));
                if (condType.equals(MORE_OR_EQUALS) | condType.equals(MORE))
                    indexes.addAll(tree.searchMore(condParamsStrs.get(0)));
                if (condType.equals(LESS_OR_EQUALS) | condType.equals(LESS))
                    indexes.addAll(tree.searchLess(condParamsStrs.get(0)));
                if (condType.equals(BETWEEN))
                    indexes.addAll(tree.searchBetween(condParamsStrs.get(0), condParamsStrs.get(1)));
                indexes.forEach(index -> rows.add(getRow(index)));
            }
        }
        return rows.stream().filter(predicate).collect(Collectors.toList());
    }

    private Row getRow(long position) {
        try {
            raf.seek(position);
            StringBuilder sb = new StringBuilder();
            char tmp;
            while ((tmp = (char) raf.readByte()) != ']') {
                sb.append(tmp);
            }
            sb.append(']');
            return Row.parseRows(table.getColumns(), sb.toString()).get(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Collection<Row> selectById(MyPredicate predicate) {
        Collection<Row> rows = new LinkedList<>();
        String pk = table.getPrimaryKeys().stream()
                .map(Column::getName)
                .map(p -> predicate.getConditions()
                        .get(predicate.getColumnNames().indexOf(p))
                        .getParams().get(0).toString())
                .collect(Collectors.joining());
        int page = hash(pk) % PAGES_COUNT;

        try {
            goToPage(page);
            int jump;
            do {
                raf.skipBytes(4);
                String jumpStr = ("" + (char) raf.readByte() + (char) raf.readByte()).trim();
                if (jumpStr.isEmpty()) jumpStr = 0 + "";
                jump = Integer.parseInt(jumpStr);
                rows.addAll(Row.parseRows(table.getColumns(), raf.readLine())
                        .stream()
                        .filter(predicate)
                        .collect(Collectors.toList()));
                if (rows.size() > 0) return rows;
                jumpToNextPage(jump);
            } while (jump > 0);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return new LinkedList<>();
    }

    private Row upd(Row oldRow, Row upRow) {
        upRow.getColumnNames().stream()
                .filter(column -> upRow.getValue(column) != null)
                .forEach(column -> oldRow.setValue(column, upRow.getValue(column)));
        return oldRow;
    }

    private void delete(long position, int delL) {
        try {
            char tmp;
            long last = 0;
            while ((tmp = (char) raf.readByte()) != '\n') {
                if (tmp == '[') {
                    long currentPosition = raf.getFilePointer();
                    Row row = getRow(currentPosition - 1);
                    table.getIndexes().entrySet()
                            .forEach(entry -> {
                                entry.getValue().remove(row.getValue(entry.getKey()).toString(), currentPosition);
                                entry.getValue().insert(row.getValue(entry.getKey()).toString(), currentPosition - delL);
                            });
                    last = raf.getFilePointer();
                }
            }
            boolean endBucket = false;
            for (long i = position; i <= last; i++) {
                raf.seek(i + delL);
                tmp = !endBucket ? (char) raf.readByte() : ' ';
                if (tmp == '\n') {
                    endBucket = true;
                    tmp = ' ';
                }
                raf.seek(i);
                raf.writeChar(tmp);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
