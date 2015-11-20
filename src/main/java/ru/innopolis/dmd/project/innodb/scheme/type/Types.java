package ru.innopolis.dmd.project.innodb.scheme.type;

import java.util.Date;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public interface Types {

    ColumnType<Integer> INT = new ColumnType<>("INT", Integer::parseInt);

    ColumnType<String> VARCHAR = new ColumnType<>("VARCHAR", s -> s);

    ColumnType<Date> DATE = new ColumnType<>("DATE", s -> new Date(Long.parseLong(s)));

    static ColumnType byName(String name) {
        try {
            return (ColumnType) Types.class.getDeclaredField(name).get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

}
