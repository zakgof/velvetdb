package com.zakgof.velvet.test.defs;

import com.zakgof.velvet.annotation.Key;

public class KeyMethodWithArgs {
    @Key
    private String key1(String arg) {
        return arg;
    }
}
