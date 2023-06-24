package com.zakgof.velvet;

import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.request.IIndexQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface IVelvetReadTransaction {
    <K, V> V singleGet(EntityDef<K,V> entityDef, K key);

    ///////////////////////////////////////////////////////////////

    <K, V> Map<K, V> batchGetMap(EntityDef<K,V> entityDef, Collection<K> keys);

    default <K, V> List<K> batchGetKeys(EntityDef<K,V> entityDef, Collection<K> keys) {
        Map<K, V> map = batchGetMap(entityDef, keys);
        return keys.stream()
                .filter(map::containsKey)
                .collect(Collectors.toList());
    }

    default <K, V> List<V> batchGetValues(EntityDef<K,V> entityDef, Collection<K> keys) {
        Map<K, V> map = batchGetMap(entityDef, keys);
        return keys.stream()
                .map(map::get)
                .collect(Collectors.toList());
    }

    ///////////////////////////////////////////////////////////////

    <K, V> Map<K, V> batchGetAllMap(IEntityDef<K,V> entityDef);

    default <K, V> List<K> batchGetAllKeys(IEntityDef<K,V> entityDef) {
        return new ArrayList<>(batchGetAllMap(entityDef).keySet());
    }

    default <K, V> List<V> batchGetAllValues(IEntityDef<K,V> entityDef) {
        return new ArrayList<>(batchGetAllMap(entityDef).values());
    }

    ///////////////////////////////////////////////////////////////

    <K, V, M> V singleGetIndex(IIndexQuery<K,V,M> indexQuery);

    <K, V, M> Map<K, V> batchGetIndexMap(IIndexQuery<K,V,M> indexQuery);

    default <K, V, M> List<K> batchGetIndexKeys(IIndexQuery<K,V,M> indexQuery) {
        return new ArrayList<>(batchGetIndexMap(indexQuery).keySet());
    }

    default <K, V, M> List<V> batchGetIndexValues(IIndexQuery<K,V,M> indexQuery) {
        return new ArrayList<>(batchGetIndexMap(indexQuery).values());
    }
}

