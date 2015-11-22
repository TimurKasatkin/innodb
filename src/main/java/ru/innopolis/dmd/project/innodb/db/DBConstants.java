package ru.innopolis.dmd.project.innodb.db;

import java.io.File;
import java.util.regex.Pattern;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public interface DBConstants {

    int PAGES_COUNT = 10;

    String[] ILLEGAL_CHARS = new String[]{"$", "[", "]", "<", ">"};

    //=================================PATTERNS=================================


    //=================================SCHEME PATTERNS=================================

    //<authors|xx><keywords|xx><journals|xx><conferences|xx><users|xx><article_author|xx><article_journal|xx><article_keyword|xx><article_conference|xx>

    Pattern DB_SCHEME_PATTERN = Pattern.compile("<(?<tablename>[a-zA-Z_]+)\\|(?<pagenumber>[0-9]+)>");

    //articles{id$INT|title$VARCHAR|publtype$VARCHAR|url$VARCHAR|year$INT}{id->pk|title->unique|title,publtypes,url->notnull}{title->articles_unique_title_index(11)}
    Pattern TABLE_SCHEME_REGEXP = Pattern.compile("(?<tablename>[a-zA-Z_]+)\\{(?<descr>[^}\\{]+)}\\{(?<constraints>[^}\\{]+)}(\\{(?<indexes>[^}\\{]+)})?");

    //{title->articles_unique_title_index(11)}
    Pattern TABLE_INDEX_SCHEME_REGEXP = Pattern.compile("(?<colnames>([a-zA-Z_]+,?)+)->(?<tablename>[a-zA-Z_]+):(?<idxtype>(unique|multi))_([A-Za-z_]+-?)+_index\\((?<idxpagenum>[0-9]+)\\)\\|?");

    Pattern TABLE_CONSTRAINT_REGEXP = Pattern.compile("(?<constraintcols>([a-zA-Z_]+,?)+)->(?<constraints>((pk|unique|notnull|fk\\((?<fktable>[A-Za-z_]+)\\((?<fkcols>([A-Za-z_]+/?)+)\\)\\)),?)+)\\|?");
    //Pattern.compile("(?<constraint>pk|fk\\((?<fktable>.+)\\((?<fkcol>.+)\\)\\)|unique|not_null)\\$?");

    Pattern TABLE_COL_SCHEME_REGEXP = Pattern.compile("(?<colname>[a-zA-Z_]+)\\$(?<coltype>[a-zA-Z_]+)\\|?");

    Pattern ROW_REGEXP = Pattern.compile("\\[(([^$\\[\\]]*\\$?)+)\\]");

    String NULL_MARKER = "\\NUL";

    //One byte characters
    //[/\\,.\[\]\{\}|"'1234567890-=+*`~!@#$%^&*()_+QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm:;<>?]+

    //=========================================PATHS=========================================
    /**
     * TODO: replace before production
     */
    String TABLES_FILES_DIRECTORY = "/home/timur/dev/workspace/innodb/src/main/resources/tables";
//            ClassLoader.getSystemResource("tables").toString().replace("file:", "");

    String DATABASE_FILE_PATH = TABLES_FILES_DIRECTORY + "/innodb.data";

    File DB_FILE = new File(DATABASE_FILE_PATH);

    //=================================COMMON PAGE STRUCTURE=================================

    int PAGE_TYPE_LENGTH = 1;

    /**
     * Length of the whole page including '\n' character
     */
    int PAGE_LENGTH = 4097 + /*for '\n' character*/1;

    //=================================DB META PAGE=================================

    int PAGES_COUNT_LENGTH = 7;

    int FIRST_FREE_PAGE_NUM_LENGTH = 7;

    //=================================HASH INDEX STRUCTURE=================================

    int INDEX_PAGE_COUNT = 23;

    int INDEX_PAGE_FREE_OFFSET_LENGTH = 4;

    int INDEX_PAGE_NEXT_PAGE_NUM_LENGTH = 10;

    int INDEX_PAGE_META_DATA_LENGTH = PAGE_TYPE_LENGTH +
            INDEX_PAGE_FREE_OFFSET_LENGTH +
            INDEX_PAGE_NEXT_PAGE_NUM_LENGTH;

    //=================================TABLE DATA PAGE STRUCTURE=================================

    int TABLE_PAGE_FREE_OFFSET_LENGTH = 4;

    int TABLE_PAGE_NEXT_PAGE_NUM_LENGTH = 10;

    int TABLE_PAGES_COUNT = 59;

    int TABLE_PAGE_META_DATA_LENGTH = PAGE_TYPE_LENGTH +
            TABLE_PAGE_FREE_OFFSET_LENGTH +
            TABLE_PAGE_NEXT_PAGE_NUM_LENGTH;

    /**
     * Length of one data page in bytes
     */
    int PAYLOAD_PAGE_LENGTH = PAGE_LENGTH - TABLE_PAGE_META_DATA_LENGTH;


}
