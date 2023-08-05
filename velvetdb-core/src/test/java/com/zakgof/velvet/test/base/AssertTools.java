package com.zakgof.velvet.test.base;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AssertTools {

    public static void assertKeyList(String expected, List<String> keys) {
        List<String> expectedKeys = expected == null ? List.of() : Arrays.asList(expected.split("\\|"));
        assertThat(keys)
                .withFailMessage("actual   %s\nexpected %s", String.join("|", keys), expected)
                .isEqualTo(expectedKeys);
    }

    public static void assertKeyListEmpty(List<String> keys) {
        assertThat(keys).isEmpty();
    }
}
