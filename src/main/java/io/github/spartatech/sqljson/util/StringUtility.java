package io.github.spartatech.sqljson.util;

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
}
