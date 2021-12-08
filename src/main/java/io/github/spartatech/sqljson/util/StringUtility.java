package io.github.spartatech.sqljson.util;

import java.util.Arrays;

public class StringUtility {

    private StringUtility() {

    }

    public static String unquote(String maybeQuoted) {
        String unquoted = maybeQuoted;
        if (unquoted.length() > 0) {
            if (unquoted.charAt(0) == '"') {
                unquoted = unquoted.substring(1);
            }
            if (unquoted.charAt(unquoted.length() - 1) == '"') {
                unquoted = unquoted.substring(0, unquoted.length() - 1);
            }
        }
        return unquoted;
    }

    public static String join(String[] array, String separator) {
        if (array == null) {
            return "";
        }
        return Arrays.stream(array).reduce((a,b) -> a.concat(separator).concat(b)).orElse("");
    }
}
