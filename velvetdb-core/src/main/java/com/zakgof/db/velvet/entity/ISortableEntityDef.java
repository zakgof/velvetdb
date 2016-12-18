package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public interface ISortableEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {

    public List<K> keys(IVelvet velvet, IRangeQuery<K, K> query);

    public K key(IVelvet velvet, ISingleReturnRangeQuery<K, K> query);

    public List<V> get(IVelvet velvet, IRangeQuery<K, K> query);

    public V get(IVelvet velvet, ISingleReturnRangeQuery<K, K> query);

}
