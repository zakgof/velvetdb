package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.entity.ISortedEntityDef;
import com.zakgof.velvet.request.IIndexDef;

import java.util.List;

public class SortedEntityDef<K extends Comparable<? super K>, V> extends EntityDef<K, V> implements ISortedEntityDef<K, V> {
    public SortedEntityDef(String kind, Class<V> valueClass, IndexInfo<K, V> key, List<IndexInfo<?, V>> indexes) {
        super(true, kind, valueClass, key, indexes);
    }

    @Override
    public IIndexDef<K, V, K> index() {
        return new IndexDef<>(this, "", this::keyOf, keyClass());
    }
}
