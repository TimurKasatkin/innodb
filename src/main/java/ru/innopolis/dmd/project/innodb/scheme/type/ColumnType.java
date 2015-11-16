package ru.innopolis.dmd.project.innodb.scheme.type;

import java.util.function.Function;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class ColumnType<T extends Comparable<T>> {

    private final String name;

    private final Function<String, T> convert;

//    private Function<String, Boolean> checkConvertibility;

    public ColumnType(String name, Function<String, T> convert) {
        this.name = name;
        this.convert = convert;
    }

    public T parse(String str) {
        return convert.apply(str);
    }

//    public boolean canConvert(String str) {
//        return checkConvertibility.apply(str);
//    }

    public String getName() {
        return name;
    }
}
