package ru.innopolis.dmd.project.innodb;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class PrototypeTableCreator {

//    public static void main(String[] args) {
//        Column empColumnPk = new Column("id", Types.INT);
//        Column studentColumnPk = new Column("id", Types.INT);
////        Student (id, name, email, address)
////        Employee(id, name, designation, Address)
//        List<Table> tables = Arrays.asList(
//                new Table("Student",
//                        Arrays.asList(studentColumnPk),
//                        Arrays.asList(new Column("name", Types.VARCHAR),
//                                new Column("email", Types.VARCHAR),
//                                new Column("address", Types.VARCHAR)), null),
//                new Table("Employee",
//                        Arrays.asList(empColumnPk),
//                        Arrays.asList(new Column("name", Types.VARCHAR),
//                                new Column("designation", Types.VARCHAR),
//                                new Column("Address", Types.VARCHAR)), null));
//        tables.forEach(table -> {
//            try (BufferedWriter writer = new BufferedWriter(
//                    new FileWriter(DBConstants.TABLES_FILES_DIRECTORY + "/" + table.getName() + ".txt"))) {
//                String scheme = table.getColumns().stream()
//                        .map(c -> (table.getPrimaryKeys().contains(c) ? "pk" : "") +
//                                "[" + c.getName() + "$" + c.getType().getName() + "]")
//                        .collect(Collectors.joining("$"));
//                writer.write(scheme);
//                writer.newLine();
//                for (int i = 0; i < DBConstants.PAGES_COUNT; i++) {
//                    writer.write(0 + repeat(" ", 4 + 2 + DBConstants.PAYLOAD_PAGE_LENGTH - 1));
//                    writer.newLine();
//                }
//                writer.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//    }
}
