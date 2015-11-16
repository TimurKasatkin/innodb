package ru.innopolis.dmd.project.innodb.db;

import java.util.regex.Pattern;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public interface DBConstants {

    int PAGES_COUNT = 10;

    /**
     * Size of one letter in bytes
     */
    byte CHAR_SIZE = Character.SIZE / 8;

    String[] ILLEGAL_CHARS = new String[]{"$", "[", "]"};

    Pattern COL_DESCRIPTION_REGEXP = Pattern.compile("((pk)?\\[[^$]+\\$[^$]+])");

    Pattern ROW_REGEXP = Pattern.compile("\\[(([^$\\[\\]]*\\$?)+)\\]");

    /**
     * Length of one data page in letters
     */
    int PAGE_LENGTH = 2042;

    /**
     * TODO: replace before production
     */
    String TABLES_FILES_DIRECTORY = "/home/timur/dev/workspace/innodb/src/main/resources/tables";
//            ClassLoader.getSystemResource("tables").toString().replace("file:", "");

    String INDEXES_FILES_DIRECTORY = TABLES_FILES_DIRECTORY + "/indexes";

    String PRIMARY_KEY_MARKER = "pk";

    String MAIN_DELIMITER = "$";

    String MAIN_DELIMITER_REGEXP = "\\" + MAIN_DELIMITER;

    int CURSOR_TO_ADD_NUM_LENGTH = 4;

    int NEXT_PAGE_JUMP_NUM_LENGTH = 2;

}
