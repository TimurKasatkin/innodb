package ru.innopolis.dmd.project.innodb.utils;

/**
 * @author Timur Kasatkin
 * @date 18.11.15.
 * @email aronwest001@gmail.com
 */
public class StringUtils {

    public static String repeat(char c, int count) {
        return repeat(c + "", count);
    }

    public static String repeat(String str, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(str);
        }
        return builder.toString();
    }

    //RS hash
    public static int hash(String str) {
        int hash = 0, i = 5, j = 3;
        for (char c : str.toCharArray()) {
            hash = (hash * i) + c;
            i *= j;
        }
        return hash;
    }

    public static String substring(String s, int from, int count) {
        return s.substring(from, from + count);
    }

}
