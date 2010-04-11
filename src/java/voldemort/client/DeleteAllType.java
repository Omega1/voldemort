package voldemort.client;

public enum DeleteAllType {

    STARTS_WITH, // string.startsWith(str, 0);
    CONTAINS, // string.indexOf(str) >= 0;
    ENDS_WITH, // string.endsWith(str);
    REGEX, // string.matches(regex)
    EL_EXPRESSION // mvel expression
}
