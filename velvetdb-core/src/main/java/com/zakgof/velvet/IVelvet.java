package com.zakgof.velvet;

import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.entity.IIndexRequest;
import com.zakgof.velvet.request.IEntityDef;

import java.util.Collection;
import java.util.Map;

public interface IVelvet {

    <K, V> void multiPut(IEntityDef<K, V> entityDef, Collection<V> values);

    <K, V> void singlePut(IEntityDef<K, V> entityDef, V value);

    <K, V> V singleGet(IEntityDef<K, V> entityDef, K key);

    <K, V> Map<K, V> multiGet(EntityDef<K, V> entityDef, Collection<K> keys);

    <K, V> Map<K, V> multiGetAll(EntityDef<K, V> entityDef);

    <K, V, M> Map<K, V> multiIndexGet(IIndexRequest<K, V, M> indexRequest);
}
