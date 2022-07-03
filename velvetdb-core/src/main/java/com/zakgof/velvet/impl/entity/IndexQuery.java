package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.request.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class IndexQuery<K, V, M> implements IBatchIndexQuery<K, V, M>, IIndexRequest<K, V, M> {

    private final IndexDef<K, V, M> indexDef;

    private int limit = -1;
    private int offset = 0;
    private boolean descending;

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    public static class Bound<K, M> implements IBound<K, M> {
        private final K key;
        private final M index;
        private final boolean inclusive;
    }

    private Bound<K, M> upper;
    private Bound<K, M> lower;

    @Override
    public IndexQuery<K, V, M> eq(M index) {
        lower = new Bound<>(null, index, true);
        upper = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, M> gt(M index) {
        lower = new Bound<>(null, index, false);
        return this;
    }

    @Override
    public IndexQuery<K, V, M> gte(M index) {
        lower = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, M> lt(M index) {
        upper = new Bound<>(null, index, false);
        return this;
    }

    @Override
    public IndexQuery<K, V, M> lte(M index) {
        upper = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, M> limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public IndexQuery<K, V, M> offset(int offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public IndexQuery<K, V, M> descending(boolean descending) {
        this.descending = descending;
        return this;
    }

    @Override
    public IReadRequest<V> first() {
        return velvet -> velvet.singleIndexGet(descending(false).limit(1));
    }

    @Override
    public IReadRequest<V> next(V value) {
        lower = new Bound<>(indexDef.entity().keyOf(value), indexDef.getter().apply(value), false);
        return velvet -> velvet.singleIndexGet(descending(false).limit(1));
    }

    @Override
    public IReadRequest<V> last() {
        return velvet -> velvet.singleIndexGet(descending(true).limit(1));
    }

    @Override
    public IReadRequest<V> prev(V value) {
        upper = new Bound<>(indexDef.entity().keyOf(value), indexDef.getter().apply(value), false);
        return velvet -> velvet.singleIndexGet(descending(true).limit(1));
    }

    @Override
    public IBatchGet<K, V> get() {
        return new MultiIndexGetRequest();
    }

    @RequiredArgsConstructor
    private class MultiIndexGetRequest extends ABatchEntityGet<K, V> {

        @Override
        protected Map<K, V> velvetMapFetcher(IVelvet velvet) {
            return velvet.multiIndexGet(IndexQuery.this);
        }
    }
}
