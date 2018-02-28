package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

public interface ISortableEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {

    public List<K> keys(IVelvet velvet, IKeyQuery<K> query);

    public K key(IVelvet velvet, ISingleReturnKeyQuery<K> query);

    public default List<V> get(IVelvet velvet, IKeyQuery<K> query) {
        return batchGetList(velvet, keys(velvet, query));
    }

    public default V get(IVelvet velvet, ISingleReturnKeyQuery<K> query) {
        K key = key(velvet, query);
        return key == null ? null : get(velvet, key);
    }

}
