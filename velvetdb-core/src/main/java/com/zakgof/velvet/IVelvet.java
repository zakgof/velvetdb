package com.zakgof.velvet;

import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.impl.entity.IIndexRequest;
import com.zakgof.velvet.entity.IEntityDef;

import java.util.Collection;
import java.util.Map;

public interface IVelvet {
    <K, V> void initialize(IEntityDef<K, V> entityDef);

    <K, V> void multiPut(IEntityDef<K, V> entityDef, Collection<V> values);

    <K, V> void singlePut(IEntityDef<K, V> entityDef, V value);

    <K, V> V singleGet(IEntityDef<K, V> entityDef, K key);

    <K, V> Map<K, V> multiGet(EntityDef<K, V> entityDef, Collection<K> keys);

    <K, V> Map<K, V> multiGetAll(EntityDef<K, V> entityDef);

    <K, V, M> Map<K, V> multiIndexGet(IIndexRequest<K, V, M> indexRequest);

    <K, V, M> V singleIndexGet(IIndexRequest<K, V, M> indexRequest);

    <K, V> void singleDelete(IEntityDef<K, V> entityDef, K key);

    <K, V> void multiDelete(IEntityDef<K, V> entityDef, Collection<K> keys);
}
