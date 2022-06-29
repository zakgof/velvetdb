package com.zakgof.velvet.impl.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.function.Function;

@RequiredArgsConstructor
@Accessors(fluent = true)
@Getter
public class IndexInfo<I, V> {
    private final String name;
    private final Class<I> type;
    private final Function<V, I> getter;
}
