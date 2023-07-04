package com.zakgof.velvet.entity;

import com.zakgof.velvet.request.IIndexDef;

public interface ISortedEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {
    IIndexDef<K, V, K> index();
}
