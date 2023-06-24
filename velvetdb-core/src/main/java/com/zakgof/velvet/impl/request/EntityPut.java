package com.zakgof.velvet.impl.request;

import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.request.IEntityPut;
import com.zakgof.velvet.request.IWriteCommand;
import lombok.RequiredArgsConstructor;

import java.util.Collection;

@RequiredArgsConstructor
public class EntityPut<K, V> implements IEntityPut<K, V> {

    private final EntityDef<K, V> entityDef;

    @Override
    public IWriteCommand value(V value) {
        return writeTxn -> writeTxn.putValue(entityDef, value);
    }

    @Override
    public IWriteCommand values(Collection<V> values) {
        return writeTxn -> writeTxn.putValues(entityDef, values);
    }
}
