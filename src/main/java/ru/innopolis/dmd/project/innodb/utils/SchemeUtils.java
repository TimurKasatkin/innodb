package ru.innopolis.dmd.project.innodb.utils;

import ru.innopolis.dmd.project.innodb.Cache;
import ru.innopolis.dmd.project.innodb.db.DBConstants;
import ru.innopolis.dmd.project.innodb.db.page.Page;
import ru.innopolis.dmd.project.innodb.db.page.PageType;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.Table;
import ru.innopolis.dmd.project.innodb.scheme.constraint.*;
import ru.innopolis.dmd.project.innodb.scheme.index.Index;
import ru.innopolis.dmd.project.innodb.scheme.index.PKIndex;
import ru.innopolis.dmd.project.innodb.scheme.index.impl.HashIndex;
import ru.innopolis.dmd.project.innodb.scheme.index.impl.HashPKIndex;
import ru.innopolis.dmd.project.innodb.scheme.type.Types;

import java.io.RandomAccessFile;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.*;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.createPage;
import static ru.innopolis.dmd.project.innodb.utils.PageUtils.getPage;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public class SchemeUtils {

    public static Table parseTable(int pageNum, String schemeDescription) {
        Matcher matcher = TABLE_SCHEME_REGEXP.matcher(schemeDescription);
        if (matcher.find()) {
            String tableName = matcher.group("tablename");
            String descriptionStr = matcher.group("descr");
            String constraintsStr = matcher.group("constraints");
            String indexesStr = matcher.group("indexes");

            List<Column> columns = parseColumns(descriptionStr);
            Map<String, Column> columnMap =
                    parallelStream(columns).collect(toMap(Column::getName, c -> c));
            List<Column> pks = new LinkedList<>();
            //PARSE INDEXES
            List<Index> indexes = new LinkedList<>();
            if (indexesStr != null) {
                matcher = TABLE_INDEX_SCHEME_REGEXP.matcher(indexesStr);
                while (matcher.find()) {
                    List<Column> indexColumns = stream(matcher.group("colnames").split(","))
                            .map(columnMap::get).collect(toList());
                    int idxpagenum = Integer.parseInt(matcher.group("idxpagenum"));
                    String idxType = matcher.group("idxtype");
                    switch (idxType) {
                        case "unique":
                            indexes.add(new HashIndex(idxpagenum, tableName, indexColumns));
                            break;
                        case "multi":
                            //TODO multivalued index
                            break;
                    }
                }
            }

            //PARSE CONSTRAINTS
            matcher = TABLE_CONSTRAINT_REGEXP.matcher(constraintsStr);
            List<Constraint> constraints = new LinkedList<>();
            List<Table> parentTables = new LinkedList<>();
            PKIndex pkIndex = null;
            while (matcher.find()) {
                String[] colNames = matcher.group("constraintcols").split(",");
                if (!stream(colNames).allMatch(columnMap::containsKey)) {
                    throw new IllegalArgumentException("Constraints description has columns which are not in ");
                }
                List<Column> constraintColumns = stream(colNames).map(columnMap::get).collect(toList());
                for (String constraintStr : matcher.group("constraints").split(",")) {
                    if (constraintStr.startsWith("pk")) {
                        if (pkIndex != null)
                            throw new IllegalArgumentException("There several separate primary key constraints");
                        pkIndex = new HashPKIndex(pageNum, tableName, constraintColumns);
                        constraints.add(new PrimaryKey(pkIndex));
                        pks.addAll(constraintColumns);
                    } else if (constraintStr.startsWith("unique")) {
                        constraints.add(new Unique(stream(indexes)
                                .filter(index -> index.getColumns().containsAll(constraintColumns))
                                .findFirst()
                                .orElseThrow(() -> new IllegalArgumentException("There is no index for such columns"))));
                    } else if (constraintStr.startsWith("fk")) {
                        String fkTableName = matcher.group("fktable");
                        //String fkCols = matcher.group("fkcols");
                        Table parentTable = Cache.getTable(fkTableName);
                        parentTables.add(parentTable);
                        constraints.add(new ForeignKey(constraintColumns, parentTable));
                    } else if (constraintStr.startsWith("notnull")) {
                        constraints.addAll(stream(constraintColumns).map(NotNull::new).collect(toList()));
                    } else
                        throw new IllegalArgumentException("Invalid constraint description format");
                }
            }
            Table table = new Table(pageNum, tableName, pks, columns, constraints, pkIndex,
                    indexes.toArray((Index<String, Long>[]) new Index[indexes.size()]));
            parentTables.forEach(parentTable -> {
                parentTable.addChildTable(table);
                table.addParentTable(parentTable);
            });
            return table;
        }
        return null;
    }

    public static List<Column> parseColumns(String colsDescription) {
        List<Column> columns = new LinkedList<>();
        Matcher matcher = TABLE_COL_SCHEME_REGEXP.matcher(colsDescription);
        while (matcher.find())
            columns.add(new Column(matcher.group("colname"), Types.byName(matcher.group("coltype"))));
        return columns;
    }

    public static Column parseColumn(String colDescription) {
        Matcher matcher = TABLE_COL_SCHEME_REGEXP.matcher(colDescription);
        if (matcher.matches())
            return new Column(matcher.group("colname"), Types.byName(matcher.group("coltype")));
        else
            throw new IllegalArgumentException("Invalid column description format");
    }

    public static Table createTable(String schemeDescription) {
        Matcher matcher = TABLE_SCHEME_REGEXP.matcher(schemeDescription);
        if (matcher.find()) {
            String tableName = matcher.group("tablename");
            String descriptionStr = matcher.group("descr");
            String constraintsStr = matcher.group("constraints");
            RandomAccessFile raf = FileUtils.createRaf("rw");
            Page schemePage = createPage(PageType.TABLE_SCHEME, raf);
            Page dbScheme = getPage(1, raf);
            int pageNum = schemePage.getNumber();
            dbScheme.insertData(MessageFormat.format("<{0}|{1}>", tableName, pageNum));
            dbScheme.serialize(raf);
            schemePage.insertData(schemeDescription);
            schemePage.serialize(raf);
            for (int i = 0; i < DBConstants.TABLE_PAGES_COUNT; i++) {
                createPage(PageType.TABLE_DATA, raf);
            }
//            String indexesStr = matcher.group("indexes");

            List<Column> columns = parseColumns(descriptionStr);
            Map<String, Column> columnMap =
                    parallelStream(columns).collect(toMap(Column::getName, c -> c));
            List<Column> pks = new LinkedList<>();

            //PARSE CONSTRAINTS
            List<Index> indexes = new LinkedList<>();
            matcher = TABLE_CONSTRAINT_REGEXP.matcher(constraintsStr);
            List<Constraint> constraints = new LinkedList<>();
            List<Table> parentTables = new LinkedList<>();
            PKIndex pkIndex = null;
            boolean hasIndexes = false;
            String indexStr = "";
            while (matcher.find()) {
                String[] colNames = matcher.group("constraintcols").split(",");
                if (!stream(colNames).allMatch(columnMap::containsKey))
                    throw new IllegalArgumentException("Constraints description has columns which are not in ");
                List<Column> constraintColumns = stream(colNames).map(columnMap::get).collect(toList());
                for (String constraintStr : matcher.group("constraints").split(",")) {
                    if (constraintStr.startsWith("pk")) {
                        if (pkIndex != null)
                            throw new IllegalArgumentException("There several separate primary key constraints");
                        pkIndex = new HashPKIndex(pageNum, tableName, constraintColumns);
                        constraints.add(new PrimaryKey(pkIndex));
                        pks.addAll(constraintColumns);
                    } else if (constraintStr.startsWith("unique")) {
                        Page uniqueIndexPage = createPage(PageType.INDEX_SCHEME, raf);
                        for (int i = 0; i < INDEX_PAGE_COUNT; i++) {
                            createPage(PageType.INDEX_DATA, raf);
                        }
                        String indexName = tableName + ":unique_" + String.join("-", colNames) + "_index";
                        String indexScheme = String.join(",", colNames) + "->" + indexName;
                        int idxPageNum = uniqueIndexPage.getNumber();
                        uniqueIndexPage.insertData(indexName);
                        uniqueIndexPage.serialize(raf);
                        if (!hasIndexes) {
                            indexStr += "{";
                            hasIndexes = true;
                        } else indexStr += "|";
                        indexStr += indexScheme + "(" + idxPageNum + ")";
                        Index index = new HashIndex(idxPageNum, tableName, constraintColumns);
                        indexes.add(index);
                        constraints.add(new Unique(index));
                    } else if (constraintStr.startsWith("fk")) {
                        String fkTableName = matcher.group("fktable");
                        Table parentTable = Cache.getTable(fkTableName);
                        if (parentTable == null)
                            throw new IllegalArgumentException("There is no parent table with name '" + fkTableName + "'");
                        parentTables.add(parentTable);
                        constraints.add(new ForeignKey(constraintColumns, parentTable));
                    } else if (constraintStr.startsWith("notnull")) {
                        constraints.addAll(stream(constraintColumns).map(NotNull::new).collect(toList()));
                    } else
                        throw new IllegalArgumentException("Invalid constraint description format");
                }
            }
            if (hasIndexes) {
                indexStr += "}";
                schemePage.insertData(indexStr);
                schemePage.serialize(raf);
            }
            Table table = new Table(pageNum, tableName, pks, columns, constraints, pkIndex,
                    indexes.toArray((Index<String, Long>[]) new Index[indexes.size()]));
            parentTables.forEach(parentTable -> {
                parentTable.addChildTable(table);
                table.addParentTable(parentTable);
            });
            Cache.tables.put(table.getName(), table);
            return table;
        }
        return null;
    }

    public static void main(String[] args) {
//        Table table = createTable("authors{id$INT|first_name$VARCHAR|last_name$VARCHAR}{id->pk|first_name,last_name->notnull,unique}");
        createScheme();
    }

    public static void createScheme() {
        List<String> tableSchemes = list(
                "articles{id$INT|title$VARCHAR|publtype$VARCHAR|url$VARCHAR|year$INT}{id->pk|title->unique|title,publtype,url->notnull}",
                "authors{id$INT|first_name$VARCHAR|last_name$VARCHAR}{id->pk|first_name,last_name->unique,notnull}",
                "conferences{id$INT|name$VARCHAR}{id->pk|name->unique,notnull}",
                "journals{id$INT|name$VARCHAR}{id->pk|name->unique,notnull}",
                "keywords{id$INT|word$VARCHAR}{id->pk|word->unique,notnull}",
                "users{id$INT|login$VARCHAR|password$VARCHAR|email$VARCHAR|role$VARCHAR}{id->pk|login,password,role->notnull|login->unique}",
                "article_author{article_id$INT|author_id$INT}{article_id,author_id->pk|article_id->fk(articles(id))|author_id->fk(authors(id))}",
                "article_conference{article_id$INT|conference_id$INT}{article_id,conference_id->pk|article_id->fk(articles(id))|conference_id->fk(conferences(id))}",
                "article_journal{article_id$INT|journal_id$INT|volume$VARCHAR|number$VARCHAR}{article_id,journal_id->pk|article_id->fk(articles(id))|journal_id->fk(journals(id))}",
                "article_keyword{article_id$INT|keyword_id$INT}{article_id,keyword_id->pk|article_id->fk(articles(id))|keyword_id->fk(keywords(id))}"
        );
        List<Table> tables = stream(tableSchemes).map(SchemeUtils::createTable).collect(Collectors.toList());
        System.out.println();
    }

}
