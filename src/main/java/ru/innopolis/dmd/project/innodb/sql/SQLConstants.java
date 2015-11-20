package ru.innopolis.dmd.project.innodb.sql;

import java.util.regex.Pattern;

/**
 * @author Timur Kasatkin
 * @date 26.10.15.
 * @email aronwest001@gmail.com
 */
public interface SQLConstants {

    Pattern SQL_INSERT_VALUES_REGEXP = Pattern.compile("(([^,]+)|('.*'))");

    Pattern SQL_COMPARISONS_REGEXP = Pattern.compile(" ?(=|<=?|>=?) ?");

    Pattern SQL_LOGICAL_OPERANDS_REGEXP = Pattern.compile("AND|and|OR|or");

}
