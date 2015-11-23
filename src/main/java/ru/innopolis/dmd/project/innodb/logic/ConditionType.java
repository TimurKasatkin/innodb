package ru.innopolis.dmd.project.innodb.logic;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;
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
    BETWEEN("between", 2, (val, params) -> 0 <= compare(val, params.get(0)) && compare(val, params.get(1)) <= 0),
    LIKE_INSENSITIVE("~*", 1, new BiPredicate<Comparable, List<Comparable>>() {

        private Pattern pattern;

        @Override
        public boolean test(Comparable val, List<Comparable> params) {
            String patternStr = ((String) params.get(0));
            if (pattern == null || !pattern.pattern().equals(patternStr))
                pattern = Pattern.compile(patternStr, Pattern.CASE_INSENSITIVE);
            return pattern.matcher((CharSequence) val).find();
        }
    });

    private final String operator;

    private final BiPredicate<Comparable, List<Comparable>> testFunc;

    private final int numOfArguments;

    ConditionType(String operator, int numOfArguments, BiPredicate<Comparable, List<Comparable>> testFunc) {
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
        return testFunc.test(val, params);
    }

    public String getOperator() {
        return operator;
    }

    public int getNumOfArgs() {
        return numOfArguments;
    }

}
