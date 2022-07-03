package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.request.IIndexDef;
import com.zakgof.velvet.request.IBatchIndexQuery;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.function.Function;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class IndexDef<K, V, M> implements IIndexDef<K, V, M> {
    private final IEntityDef<K, V> entity;
    private final String name;
    private final Function<V, M> getter;
    private final Class<M> type;

    @Override
    public IBatchIndexQuery<K, V, M> query() {
        return new IndexQuery<>(this);
    }
}
