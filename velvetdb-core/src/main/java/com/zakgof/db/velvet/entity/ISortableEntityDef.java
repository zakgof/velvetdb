package com.zakgof.db.velvet.entity;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

public interface ISortableEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {

    public List<K> queryKeys(IVelvet velvet, IKeyQuery<K> query);

    public K queryKey(IVelvet velvet, ISingleReturnKeyQuery<K> query);

    public default List<V> queryList(IVelvet velvet, IKeyQuery<K> query) {
        return batchGetList(velvet, queryKeys(velvet, query));
    }

    public default Map<K, V> queryMap(IVelvet velvet, IKeyQuery<K> query) {
        return batchGetMap(velvet, queryKeys(velvet, query));
    }

    public default V queryValue(IVelvet velvet, ISingleReturnKeyQuery<K> query) {
        K key = queryKey(velvet, query);
        return key == null ? null : get(velvet, key);
    }

}
