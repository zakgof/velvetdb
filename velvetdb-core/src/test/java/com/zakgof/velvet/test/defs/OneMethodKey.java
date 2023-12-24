package com.zakgof.velvet.test.defs;

import com.zakgof.velvet.annotation.Key;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OneMethodKey {

    private final String value;

    @Key
    private String key() {
        return value;
    }
}
