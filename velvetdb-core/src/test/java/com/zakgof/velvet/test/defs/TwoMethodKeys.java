package com.zakgof.velvet.test.defs;

import com.zakgof.velvet.annotation.Key;

public class TwoMethodKeys {
    @Key
    private String key1() {
        return "key1";
    }

    @Key
    private String key2() {
        return "key2";
    }
}
