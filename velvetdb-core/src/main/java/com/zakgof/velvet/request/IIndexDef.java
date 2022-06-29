package com.zakgof.velvet.request;

import java.util.function.Function;

public interface IIndexDef<K, V, I> {

    String name();

    IEntityDef<K, V> entity();

    Function<V, I> getter();

    Class<I> type();

    IIndexQuery<K, V, I> query();
}
