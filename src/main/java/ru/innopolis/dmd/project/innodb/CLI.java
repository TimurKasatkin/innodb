package ru.innopolis.dmd.project.innodb;

import ru.innopolis.dmd.project.innodb.db.QueryProcessor;
import ru.innopolis.dmd.project.innodb.scheme.Row;

import java.util.Collection;
import java.util.Scanner;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class CLI {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String query;
        String instructions = "Input query or 'EXIT' to stop program";
        System.out.println(instructions);
        query = scanner.nextLine();
        QueryProcessor queryProcessor = Cache.queryProcessor;
        while (!query.equalsIgnoreCase("EXIT")) {
            Collection<Row> rows = queryProcessor.execute(query);
            System.out.println(rows == null ? "No result" : "Result: " + rows);
            System.out.println(instructions);
            query = scanner.nextLine();
        }
        scanner.close();
    }

}
