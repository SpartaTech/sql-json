package io.github.spartatech.sqljson.vo;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum MultiListType {
    MATCH_ANY("matchAny"),
    MATCH_ALL("matchAll");

    private String functionName;

    private MultiListType(String functionName) {
        this.functionName = functionName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public static String allOptionsAsString() {
        return Arrays.stream(MultiListType.values())
                .map(MultiListType::getFunctionName)
                .collect(Collectors.joining(", "));
    }

    public static MultiListType fromValue(String value) {
        return Arrays.stream(MultiListType.values())
                .filter( i -> i.getFunctionName().equals(value))
                .findFirst()
                .orElseThrow(() -> new EnumConstantNotPresentException(MultiListType.class, value));
    }
}
