package ru.innopolis.dmd.project.innodb;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class Tuple {

    private Comparable[] values;

    public Tuple(Comparable... values) {
        this.values = values;
    }

    public Comparable get(int index) {
        return values[index];
    }
}
