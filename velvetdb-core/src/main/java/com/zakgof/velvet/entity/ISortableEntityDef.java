package com.zakgof.velvet.entity;

import com.zakgof.velvet.request.IIndexDef;

public interface ISortableEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {
    public <I> IIndexDef<K, V, K> index();
}
