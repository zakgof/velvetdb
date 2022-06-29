package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.request.IEntityPut;
import com.zakgof.velvet.request.IWriteRequest;

import java.util.Collection;

public class EntityPut<K, V> implements IEntityPut<K, V> {

    private final EntityDef<K, V> entityDef;

    public EntityPut(EntityDef<K, V> entityDef) {
        this.entityDef = entityDef;
    }

    @Override
    public IWriteRequest value(V value) {
        return new SinglePutRequest<K, V>(entityDef, value);
    }

    @Override
    public IWriteRequest values(Collection<V> values) {
        return new MultiPutRequest<K, V>(entityDef, values);
    }
}
