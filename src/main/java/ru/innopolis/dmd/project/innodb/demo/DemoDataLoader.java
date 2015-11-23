package ru.innopolis.dmd.project.innodb.demo;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.dml.Insert;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.utils.RowUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * @author Timur Kasatkin
 * @date 23.11.15.
 * @email aronwest001@gmail.com
 */
public class DemoDataLoader {

    public static void main(String[] args) {
        try (
                BufferedReader reader = new BufferedReader(new FileReader(ClassLoader.getSystemResource("data/articles_demo_data.txt").toString().replace("file:", "")));
                BufferedReader reader1 = new BufferedReader(new FileReader(ClassLoader.getSystemResource("data/journals_demo_data.txt").toString().replace("file:", "")));
                BufferedReader reader2 = new BufferedReader(new FileReader(ClassLoader.getSystemResource("data/conferences_demo_data.txt").toString().replace("file:", "")));
                BufferedReader reader3 = new BufferedReader(new FileReader(ClassLoader.getSystemResource("data/keywords_demo_data.txt").toString().replace("file:", "")));
                BufferedReader reader4 = new BufferedReader(new FileReader(ClassLoader.getSystemResource("data/authors_demo_data.txt").toString().replace("file:", "")));
        ) {
//            insert("articles",reader);
//            insert("journals", reader1);
//            insert("conferences",reader2);
//            insert("keywords",reader3);
            insert("authors", reader4);
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        Scan articles = new Scan("articles");
//        System.out.println(articles.loadAll().size());
//        while (articles.hasNext())
//            System.out.println(articles.next());
    }

    private static void insert(String tableName, BufferedReader reader) throws IOException {
        String s = reader.readLine();
        Table table = Cache.getTable(tableName);
        List<Column> cols = table.getColumns();
        List<Row> rows = RowUtils.parseRows(s, cols);
        rows.forEach(row -> new Insert(row, table).execute());
    }

}
