package ru.innopolis.dmd.project.innodb.logic;

import java.util.function.BiPredicate;

/**
 * @author Timur Kasatkin
 * @date 19.11.15.
 * @email aronwest001@gmail.com
 */
public enum LogicalOperation {
    AND((b1, b2) -> b1 && b2), OR((b1, b2) -> b1 || b2);

    private final BiPredicate<Boolean, Boolean> testFunc;

    LogicalOperation(BiPredicate<Boolean, Boolean> testFunc) {
        this.testFunc = testFunc;
    }

    public boolean test(boolean b1, boolean b2) {
        return testFunc.test(b1, b2);
    }
}
