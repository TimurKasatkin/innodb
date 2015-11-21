package ru.innopolis.dmd.project.innodb.db;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
//public class StorageManager implements AutoCloseable {
//
//    private Table table;
//    private RandomAccessFile raf;
//    /**
//     * in bytes!!!
//     */
//    private int schemaLineLength;
//
//    private File file;
//
//    public StorageManager(File file) throws IOException {
//        if (!file.exists())
//            throw new IllegalArgumentException("File does not exists");
//        if (file.isDirectory())
//            throw new IllegalArgumentException("Directory passed");
//        this.file = file;
//        this.raf = new RandomAccessFile(file, "rwd");
//        raf.seek(0);
//        String scheme = raf.readLine();
//        this.schemaLineLength = scheme.length() + 1;
//        this.table = Table.parseTable(file.getName().replace(".txt", ""), scheme);
//    }
//
//    //74    [5$name 5$designation 5$address 5][15$name 15$designation 15$address 15]
//    //SELECT * FROM Employee
//    //INSERT INTO Employee (id,name,designation,Address) VALUES (16,'name 16','designation 16','Address 16')
//    public void insert(Row row) {
//        System.out.print("TRYING INSERT: " + row + "... ");
//        List<String> colNames = table.getColumns()
//                .stream().map(Column::getName).collect(toList());
//        //TODO auto id calculation (if it is possible)
//        if (!row.getColumnNames().stream().allMatch(colNames::contains))
//            throw new IllegalArgumentException("Row contains not all columns");
//        try {
//            List<String> pkNames = table.getPrimaryKeys()
//                    .stream().map(Column::getName).collect(toList());
//            String pkValuesConcatenation = String.join("",
//                    pkNames.stream()
//                            .map(row::getValue)
//                            .map(Object::toString)
//                            .collect(toList()));
//            int pageIndex = hash(pkValuesConcatenation) % PAGES_COUNT;
//            String formattedRow = format(row);
//            long offset = setToPage(pageIndex);
//
//            while (true) {
//                long currentPosition = raf.getFilePointer();
//                String posToJumpStr = readChars(FREE_OFFSET_LENGTH).trim();
//                if (posToJumpStr.isEmpty())
//                    posToJumpStr = "0";
//                int freeOffset = Integer.parseInt(posToJumpStr);
//                if (freeOffset + formattedRow.length() > PAYLOAD_PAGE_LENGTH) {
//                    String jumpToPage = readChars(2).trim();
//                    if (jumpToPage.length() == 0)
//                        offset = createNewPage(currentPosition);
//                    else offset = getPageContPointer(Integer.parseInt(jumpToPage.trim()));
//                } else {
//                    raf.skipBytes(NEXT_PAGE_NUM_LENGTH + freeOffset);
//                    long posToTree = raf.getFilePointer();
//                    raf.writeBytes(formattedRow);
//                    raf.seek(offset);
//                    raf.writeBytes("    ");
//                    raf.seek(raf.getFilePointer() - FREE_OFFSET_LENGTH);
//                    raf.writeBytes((freeOffset + formattedRow.length()) + "");
//                    Map<String, MultivaluedBPlusTreeIndex<String, Long>> indexes = table.getIndexesMap();
////                    List<Map.Entry<String, MultivaluedBPlusTreeIndex<String, Long>>> indexes = indexes.entrySet().stream()
////                            .filter(entry -> row.getColumnNames().contains(entry.getKey()))
////                            .collect(toList());
////                    indexes.forEach((key, value) ->
////                            value.insert(row.getValue(key).toString(), posToTree));
////                    table.getIndexesMap().keySet().forEach(index -> table
////                            .getIndexesMap().get(index)
////                            .insert(row.getValue(index).toString(), posToTree));
//                    System.out.println("ROW INSERTED.");
//                    return;
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Table getTable() {
//        return table;
//    }
//
//    public List<Row> select(RowPredicate predicate) {
//        if (predicate.getColumnNames().stream()
//                .anyMatch(table.getIndexesMap()::containsKey)) {
//            return selectByIndex(predicate);
//        }
//        if (table.getPrimaryKeys().stream()
//                .map(Column::getName)
//                .allMatch(predicate.getColumnNames()::contains)) {
//            return selectById(predicate);
//        }
//        List<Row> rows = new LinkedList<>();
//        try {
//            setToPage(0);
//            while (raf.getFilePointer() < raf.length()) {
//                String page = raf.readLine();
//                rows.addAll(parseRows(table.getColumns(), page)
//                        .stream()
//                        .filter(predicate)
//                        .collect(toList()));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return rows;
//    }
//
//    public void update(
//            //Contains Field->New value
//            //for all fields, which we want to update
//            Row row, RowPredicate predicate) {
//        Collection<Row> delRows = delete(predicate);
//        delRows.forEach(r -> insert(upd(r, row)));
//    }
//
//    //TODO fix
//    public List<Row> delete(RowPredicate predicate) {
//        List<Row> delRows = new LinkedList<>();
//        try {
//            setToPage(0);
//            while (raf.getFilePointer() < raf.length()) {
//                char tmp = (char) raf.readByte();
//                if (tmp == '[') {
//                    long position = raf.getFilePointer() - 1;
//                    Row row = getRow(position);
//                    if (predicate.test(row)) {
//                        delete(position, row.toString().length());
//                        table.getIndexesMap().entrySet()
//                                .forEach(entry -> entry.getValue()
//                                        .remove(row.getValue(entry.getKey()).toString(), position));
//                        delRows.add(row);
//                    }
//                }
//
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return delRows;
//    }
//
//    private long createNewPage(long lastPageStart) throws IOException {
//        long pos = file.length();
//        raf.seek(pos);
//        raf.writeBytes(0 + repeat(" ", PAGE_LENGTH - 2) + '\n');
//        long jump = (pos - lastPageStart) / (PAGE_LENGTH);
//        raf.seek(lastPageStart + 4);
//        raf.writeBytes(jump + "");
//        raf.seek(pos);
//        return pos;
//    }
//
//    private long getPageContPointer(int jump) throws IOException {
//        raf.seek(raf.getFilePointer() + PAGE_LENGTH * jump - 5);
//        return raf.getFilePointer();
//    }
//
//    /**
//     * Go to page with specified number
//     * and return number of bytes before the page starting from beginning of the file.
//     *
//     * @param num number of page
//     * @return number of bytes before the page starting from beginning of the file.
//     * @throws IOException
//     */
//    private long setToPage(int num) throws IOException {
//        long offset = schemaLineLength + num * (PAGE_LENGTH);
//        raf.seek(offset);
//        return offset;
//    }
//
//    private String format(Row row) {
//        return "[" + table.getColumns().stream()
//                .map(Column::getName)
//                .map(row::getValue)
//                .map(Object::toString)
//                .collect(Collectors.joining("$")) + "]";
//    }
//
//    @Override
//    public void close() {
//        if (raf != null) {
//            try {
//                raf.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private List<Row> selectByIndex(RowPredicate predicate) {
//        List<Row> rows = new LinkedList<>();
//        List<String> colNames = predicate.getColumnNames();
//        List<Condition> conditions = predicate.getConditions();
//        for (int i = 0; i < colNames.size(); i++) {
//            if (table.getIndexesMap().containsKey(colNames.get(i))) {
//                MultivaluedBPlusTreeIndex<String, Long> tree = table.getIndexesMap().get(colNames.get(i));
//                Condition condition = conditions.get(i);
//                ConditionType condType = condition.getConditionType();
//                List<String> condParamsStrs = condition.getParams().stream()
//                        .map(Object::toString)
//                        .collect(toList());
//                List<Long> indexes = new LinkedList<>();
//                if (condType.equals(EQUALS) | condType.equals(MORE_OR_EQUALS)
//                        | condType.equals(LESS_OR_EQUALS))
//                    indexes.addAll(tree.search(condParamsStrs.get(0)));
//                if (condType.equals(MORE_OR_EQUALS) | condType.equals(MORE))
//                    indexes.addAll(tree.searchMore(condParamsStrs.get(0)));
//                if (condType.equals(LESS_OR_EQUALS) | condType.equals(LESS))
//                    indexes.addAll(tree.searchLess(condParamsStrs.get(0)));
//                if (condType.equals(BETWEEN))
//                    indexes.addAll(tree.searchBetween(condParamsStrs.get(0), condParamsStrs.get(1)));
//                indexes.forEach(index -> rows.add(getRow(index)));
//            }
//        }
//        return rows.stream().filter(predicate).collect(Collectors.toList());
//    }
//
//    private List<Row> selectById(RowPredicate predicate) {
//        List<Row> rows = new LinkedList<>();
//        String pk = table.getPrimaryKeys().stream()
//                .map(Column::getName)
//                .map(p -> predicate.getConditions()
//                        .get(predicate.getColumnNames().indexOf(p))
//                        .getParams().get(0).toString())
//                .collect(Collectors.joining());
//        int page = hash(pk) % PAGES_COUNT;
//
//        try {
//            setToPage(page);
//            int jump;
//            do {
//                raf.skipBytes(4);
//                String jumpStr =
////                        ("" + (char) raf.readByte() + (char) raf.readByte())
//                        readChars(2).trim();
//                if (jumpStr.isEmpty()) jumpStr = "0";
//                jump = Integer.parseInt(jumpStr);
//                rows.addAll(parseRows(table.getColumns(), raf.readLine())
//                        .stream()
//                        .filter(predicate)
//                        .collect(Collectors.toList()));
//                if (rows.size() > 0) return rows;
//                getPageContPointer(jump);
//            } while (jump > 0);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return new LinkedList<>();
//    }
//
//    private Row getRow(long position) {
//        try {
//            raf.seek(position);
//            StringBuilder sb = new StringBuilder();
//            char tmp;
//            while ((tmp = (char) raf.readByte()) != ']') {
//                sb.append(tmp);
//            }
//            sb.append(']');
//            return parseRows(table.getColumns(), sb.toString()).get(0);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    private Row upd(Row oldRow, Row upRow) {
//        upRow.getColumnNames().stream()
//                .filter(column -> upRow.getValue(column) != null)
//                .forEach(column -> oldRow.setValue(column, upRow.getValue(column)));
//        return oldRow;
//    }
//
//    private void delete(long position, int delL) {
//        try {
//            char tmp;
//            long last = 0;
//            while ((tmp = (char) raf.readByte()) != '\n') {
//                if (tmp == '[') {
//                    long currentPosition = raf.getFilePointer();
//                    Row row = getRow(currentPosition - 1);
//                    table.getIndexesMap().entrySet()
//                            .forEach(entry -> {
//                                entry.getValue().remove(row.getValue(entry.getKey()).toString(), currentPosition);
//                                entry.getValue().insert(row.getValue(entry.getKey()).toString(), currentPosition - delL);
//                            });
//                    last = raf.getFilePointer();
//                }
//            }
//            boolean endBucket = false;
//            for (long i = position; i <= last; i++) {
//                raf.seek(i + delL);
//                tmp = !endBucket ? (char) raf.readByte() : ' ';
//                if (tmp == '\n') {
//                    endBucket = true;
//                    tmp = ' ';
//                }
//                raf.seek(i);
//                raf.writeChar(tmp);
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private String readChars(int count, RandomAccessFile raf) throws IOException {
//        if (raf == null) raf = this.raf;
//        String result = "";
//        for (int i = 0; i < count; i++)
//            result += (char) raf.readByte();
//        return result;
//    }
//
//    private String readChars(int count) throws IOException {
//        return readChars(count, null);
//    }
//
//}
