package ru.innopolis.dmd.project.innodb.relalgrebra;

import ru.innopolis.dmd.project.innodb.Row;

import java.util.List;
import java.util.stream.Collectors;

import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class Project implements RelationalOperator {

    private final List<String> colNames;
    private final RelationalOperator operator;

    public Project(List<String> colNames, RelationalOperator operator) {
        this.colNames = colNames;
        this.operator = operator;
    }

    public static void main(String[] args) {
        Project project = new Project(list("articles_id", "articles_title"), new Scan("articles"));
        while (project.hasNext())
            System.out.println(project.next());
    }

    @Override
    public Row next() {
        Row next = operator.next();
        return new Row(map(colNames.stream()
                .map(s -> entry(s, next.getValue(s)))
                .collect(Collectors.toList())));
    }

    @Override
    public boolean hasNext() {
        return operator.hasNext();
    }

    @Override
    public void reset() {
        operator.reset();
    }
}
