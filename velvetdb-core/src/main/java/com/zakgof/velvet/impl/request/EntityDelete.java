package com.zakgof.velvet.impl.request;

import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.request.IEntityDelete;
import com.zakgof.velvet.request.IWriteCommand;
import lombok.RequiredArgsConstructor;

import java.util.Collection;

@RequiredArgsConstructor
public class EntityDelete<K, V> implements IEntityDelete<K, V> {

    private final IEntityDef<K, V> entityDef;

    @Override
    public IWriteCommand key(K key) {
        return writeTxn -> writeTxn.deleteKey(entityDef, key);
    }

    @Override
    public IWriteCommand keys(Collection<K> keys) {
        return writeTxn -> writeTxn.deleteKeys(entityDef, keys);
    }

    @Override
    public IWriteCommand value(V value) {
        return writeTxn -> writeTxn.deleteValue(entityDef, value);
    }

    @Override
    public IWriteCommand values(Collection<V> values) {
        return writeTxn -> writeTxn.deleteValues(entityDef, values);
    }
}
