package com.zakgof.db.velvet.entity;

import java.util.Collection;
import java.util.List;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet;

public abstract class AEntityDef<K, V> implements IEntityDef<K, V> {

    @Override
    public List<V> get(IVelvet velvet, Collection<K> keys) {
        return Stream.of(keys).map(key -> get(velvet, key)).collect(Collectors.toList());
    }

    @Override
    public List<V> get(IVelvet velvet) {
        return get(velvet, keys(velvet));
    }


    @Override
    public void deleteValue(IVelvet velvet, V value) {
        deleteKey(velvet, keyOf(value));
    }

    @Override
    public boolean equals(V value1, V value2) {
        return keyOf(value1).equals(keyOf(value2));
    }

}
