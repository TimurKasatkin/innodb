package ru.innopolis.dmd.project.innodb.sql;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * @author Timur Kasatkin
 * @date 24.10.15.
 * @email aronwest001@gmail.com
 */
public enum ConditionType {
    EQUALS("=", 1, (val, params) -> compare(val, params.get(0)) == 0),
    LESS("<", 1, (val, params) -> compare(val, params.get(0)) < 0),
    LESS_OR_EQUALS("<=", 1, (val, params) -> compare(val, params.get(0)) <= 0),
    MORE(">", 1, (val, params) -> compare(val, params.get(0)) > 0),
    MORE_OR_EQUALS(">=", 1, (val, params) -> compare(val, params.get(0)) >= 0),
    BETWEEN("between", 2, (val, params) -> 0 <= compare(val, params.get(0)) && compare(val, params.get(1)) <= 0);

    private final String operator;

    private final BiFunction<Comparable, List<Comparable>, Boolean> testFunc;

    private final int numOfArguments;

    ConditionType(String operator, int numOfArguments, BiFunction<Comparable, List<Comparable>, Boolean> testFunc) {
        this.operator = operator;
        this.numOfArguments = numOfArguments;
        this.testFunc = testFunc;
    }

    public static ConditionType byOperator(String operator) {
        return Stream.of(values()).filter(cT -> cT.operator.equals(operator)).findFirst().orElse(null);
    }

    private static int compare(Comparable o1, Comparable o2) {
        return o1.compareTo(o2);
    }

    public boolean matches(Comparable val, List<Comparable> params) {
        return testFunc.apply(val, params);
    }

    public String getOperator() {
        return operator;
    }

    public int getNumOfArgs() {
        return numOfArguments;
    }
}
