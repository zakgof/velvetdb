package com.zakgof.velvet.test.defs;

import com.zakgof.velvet.annotation.Key;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OneFieldKey {
    @Key
    private final String key;
}
