package ru.innopolis.dmd.project.innodb.scheme.type;

import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.NULL_MARKER;


/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public interface Types {

    ColumnType<Integer> INT = new ColumnType<>("INT", s -> s.equals(NULL_MARKER) || s.isEmpty() ? null : Integer.parseInt(s));

    ColumnType<String> VARCHAR = new ColumnType<>("VARCHAR", identity());

    ColumnType<Date> DATE = new ColumnType<>("DATE", s -> s.equals(NULL_MARKER) || s.isEmpty() ? null : new Date(Long.parseLong(s)));

    Map<String, ColumnType> TYPES = Stream.of(INT, VARCHAR, DATE)
            .collect(toMap(ColumnType::getName, identity()));

    static ColumnType byName(String name) {
        try {
            return (ColumnType) Types.class.getDeclaredField(name).get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

}
