package com.zakgof.velvet;

import com.zakgof.velvet.entity.IEntityDef;

import java.util.Collection;

public interface IVelvetWriteTransaction {

    <K, V> void putValue(IEntityDef<K, V> entityDef, V value);

    default <K, V> void putValues(IEntityDef<K, V> entityDef,  Collection<V> values) {
        values.forEach(value -> putValue(entityDef, value));
    }

    ///////////////////////////////////////////////////////////////

    <K, V> void deleteKey(IEntityDef<K,V> entityDef, K key);

    default <K, V> void deleteKeys(IEntityDef<K,V> entityDef, Collection<K> keys) {
        keys.forEach(key -> deleteKey(entityDef, key));
    }

    default <K, V> void deleteValue(IEntityDef<K,V> entityDef, V value) {
        deleteKey(entityDef, entityDef.keyOf(value));
    }

    default <K, V> void deleteValues(IEntityDef<K,V> entityDef, Collection<V> values) {
        values.forEach(value -> deleteValue(entityDef, value));
    }
}
