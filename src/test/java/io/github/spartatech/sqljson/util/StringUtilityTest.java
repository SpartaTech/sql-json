package io.github.spartatech.sqljson.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class StringUtilityTest {

    @ParameterizedTest
    @MethodSource("joinParameters")
    public void test_join(String[] arr, String result) {
        assertEquals(result, StringUtility.join(arr, "."));
    }

    @Test
    public void test_join_multi_character_delimiter() {
        assertEquals("a, b, c", StringUtility.join(new String[] {"a", "b", "c"}, ", "));
    }

    public static Stream<Arguments> joinParameters() {
        return Stream.of(
                arguments(new String[]{"a"}, "a"),
                arguments(new String[]{"a", "b"}, "a.b"),
                arguments(new String[]{"a", "b", "c"}, "a.b.c"),
                arguments(new String[]{}, ""),
                arguments(null, "")
        );
    }
}
