package ru.innopolis.dmd.project.innodb.db;

import java.util.stream.Stream;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public enum PageType {

    FREE_PAGE('0'),
    DATABASE_META('1'),
    DATABASE_SCHEME('2'),
    TABLE_SCHEME('3'),
    TABLE_DATA('4'),
    INDEX_SCHEME('5'),
    INDEX_DATA('6');

    public final char marker;

    PageType(char marker) {
        this.marker = marker;
    }

    public static PageType byMarker(char marker) {
        return Stream.of(values())
                .filter(pt -> pt.marker == marker)
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }


}
