package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.IVelvet;
import com.zakgof.velvet.request.IBatchEntityGet;
import com.zakgof.velvet.request.IIndexQuery;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class IndexQuery<K, V, M> implements IIndexQuery<K, V, M>, IIndexRequest<K, V, M> {

    private final IndexDef<K, V, M> indexDef;

    private int limit = -1;
    private int offset = 0;
    private boolean descending;

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    public static class Bound<K, V, M> implements IBound<K, V, M> {
        private final K key;
        private final M index;
        private final boolean inclusive;
    }

    private Bound<K, V, M> upper;
    private Bound<K, V, M> lower;

    @Override
    public IIndexQuery<K, V, M> eq(M index) {
        lower = new Bound<>(null, index, true);
        upper = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> gt(M index) {
        lower = new Bound<>(null, index, false);
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> gte(M index) {
        lower = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> lt(M index) {
        upper = new Bound<>(null, index, false);
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> lte(M index) {
        upper = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> offset(int offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> descending(boolean descending) {
        this.descending = descending;
        return this;
    }

    @Override
    public IIndexQuery<K, V, M> first() {
        return descending(false).limit(1).offset(0);
    }

    @Override
    public IIndexQuery<K, V, M> next(V value) {
        lower = new Bound<>(indexDef.entity().keyOf(value), null, false);
        return descending(false).limit(1).offset(0);
    }

    @Override
    public IIndexQuery<K, V, M> last() {
        return descending(true).limit(1).offset(0);
    }

    @Override
    public IIndexQuery<K, V, M> prev(V value) {
        upper = new Bound<>(indexDef.entity().keyOf(value), null, false);
        return descending(true).limit(1).offset(0);
    }

    @Override
    public IBatchEntityGet<K, V> get() {
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
