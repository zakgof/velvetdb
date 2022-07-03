package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.request.IBatchGet;
import com.zakgof.velvet.request.IEntityGet;
import com.zakgof.velvet.request.IReadRequest;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;

@RequiredArgsConstructor
public class EntityGet<K, V> implements IEntityGet<K, V> {

    private final EntityDef<K, V> entityDef;

    @Override
    public IReadRequest<V> key(K key) {
        return new SingleGetRequest(key);
    }

    @Override
    public IBatchGet<K, V> keys(Collection<K> keys) {
        return new MultiGetRequest(keys);
    }

    @Override
    public IBatchGet<K, V> all() {
        return new MultiGetAllRequest();
    }

    @RequiredArgsConstructor
    private class SingleGetRequest implements IReadRequest<V> {

        private final K key;

        @Override
        public V execute(IVelvet velvet) {
            return velvet.singleGet(entityDef, key);
        }
    }

    @RequiredArgsConstructor
    private class MultiGetRequest extends ABatchEntityGet<K, V> {

        private final Collection<K> keys;

        @Override
        protected Map<K, V> velvetMapFetcher(IVelvet velvet) {
            return velvet.multiGet(entityDef, keys);
        }
    }

    @RequiredArgsConstructor
    private class MultiGetAllRequest extends ABatchEntityGet<K, V> {

        @Override
        protected Map<K, V> velvetMapFetcher(IVelvet velvet) {
            return velvet.multiGetAll(entityDef);
        }

    }
}
