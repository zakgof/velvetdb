package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.List;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet;

public abstract class AEntityDef<K, V> implements IEntityDef<K, V> {

    @Override
    public boolean equals(V value1, V value2) {
        return keyOf(value1).equals(keyOf(value2));
    }

     @Override
    public List<V> batchGetList(IVelvet velvet, List<K> keys) {
        return new ArrayList<>(batchGetMap(velvet, keys).values());
    }

     @Override
    public List<V> batchGetAllList(IVelvet velvet) {
        return new ArrayList<>(batchGetAllMap(velvet).values());
    }

    @Override
    public  void deleteValue(IVelvet velvet, V value) {
        deleteKey(velvet, keyOf(value));
    }

    @Override
    public  void batchDeleteValues(IVelvet velvet, List<V> values) {
        batchDeleteKeys(velvet, Stream.of(values).map(this::keyOf).collect(Collectors.toList()));
    }

}
