package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.request.IBatchGet;
import com.zakgof.velvet.request.IReadRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract class ABatchEntityGet<K, V> implements IBatchGet<K, V> {

    @Override
    public IReadRequest<List<K>> asKeyList() {
        return velvet -> new ArrayList<>(velvetMapFetcher(velvet).keySet());
    }

    @Override
    public IReadRequest<List<V>> asValueList() {
        return velvet -> new ArrayList<>(velvetMapFetcher(velvet).values());
    }

    @Override
    public IReadRequest<Map<K, V>> asMap() {
        return this::velvetMapFetcher;
    }

    protected abstract Map<K, V> velvetMapFetcher(IVelvet velvet);
}
