package ru.innopolis.dmd.project.innodb.scheme;

import ru.innopolis.dmd.project.innodb.scheme.type.ColumnType;

import java.util.stream.Stream;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.ILLEGAL_CHARS;

/**
 * @author Timur Kasatkin
 * @date 18.10.15.
 * @email aronwest001@gmail.com
 */
public class Column {

    private String name;

    private ColumnType type;

    public Column(String name, ColumnType type) {
        if (name == null || name.isEmpty() || Stream.of(ILLEGAL_CHARS).anyMatch(name::contains))
            throw new IllegalArgumentException("Invalid name");
        if (type == null)
            throw new IllegalArgumentException("Type can not be null");
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return name.equals(column.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
