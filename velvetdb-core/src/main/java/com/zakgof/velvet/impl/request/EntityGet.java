package com.zakgof.velvet.impl.request;

import com.zakgof.velvet.impl.entity.EntityDef;
import com.zakgof.velvet.request.IBatchGet;
import com.zakgof.velvet.request.IEntityGet;
import com.zakgof.velvet.request.IReadCommand;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class EntityGet<K, V> implements IEntityGet<K, V> {

    private final EntityDef<K, V> entityDef;

    @Override
    public IReadCommand<V> key(K key) {
        return readTxn -> readTxn.singleGet(entityDef, key);
    }

    @Override
    public IBatchGet<K, V> keys(Collection<K> keys) {
        return new IBatchGet<K, V>() {
            @Override
            public IReadCommand<List<K>> asKeyList() {
                return readTxn -> readTxn.batchGetKeys(entityDef, keys);
            }

            @Override
            public IReadCommand<List<V>> asValueList() {
                return readTxn -> readTxn.batchGetValues(entityDef, keys);
            }

            @Override
            public IReadCommand<Map<K, V>> asMap() {
                return readTxn -> readTxn.batchGetMap(entityDef, keys);
            }
        };
    }

    @Override
    public IBatchGet<K, V> all() {
        return new IBatchGet<K, V>() {
            @Override
            public IReadCommand<List<K>> asKeyList() {
                return readTxn -> readTxn.batchGetAllKeys(entityDef);
            }

            @Override
            public IReadCommand<List<V>> asValueList() {
                return readTxn -> readTxn.batchGetAllValues(entityDef);
            }

            @Override
            public IReadCommand<Map<K, V>> asMap() {
                return readTxn -> readTxn.batchGetAllMap(entityDef);
            }
        };
    }
}
