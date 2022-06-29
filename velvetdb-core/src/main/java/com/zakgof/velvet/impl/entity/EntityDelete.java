package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.request.IEntityDelete;
import com.zakgof.velvet.request.IWriteRequest;
import lombok.RequiredArgsConstructor;

import java.util.Collection;

@RequiredArgsConstructor
public class EntityDelete<K, V>  implements IEntityDelete<K, V> {
    private final IEntityDef<K, V> entityDef;

    @Override
    public IWriteRequest key(K key) {
        return velvet -> velvet.<K, V>singleDelete(entityDef, key);
    }

    @Override
    public IWriteRequest keys(Collection<K> keys) {
        return null;
    }

    @Override
    public IWriteRequest value(V value) {
        return null;
    }

    @Override
    public IWriteRequest values(Collection<V> values) {
        return null;
    }
}
