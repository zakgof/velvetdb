package com.zakgof.velvet.impl.index;

import com.zakgof.velvet.request.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class IndexQuery<K, V, I> implements IBatchIndexQuery<K, V, I> {

    private final IndexDef<K, V, I> indexDef;

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

    private final Bound<K, I>[] bounds = new Bound[2];

    @Override
    public IndexQuery<K, V, I> eq(I index) {
        bounds[0] = new Bound<>(null, index, true);
        bounds[1] = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> gt(I index) {
        bounds[0] = new Bound<>(null, index, false);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> gte(I index) {
        bounds[0] = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> lt(I index) {
        bounds[1] = new Bound<>(null, index, false);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> lte(I index) {
        bounds[1] = new Bound<>(null, index, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> gtK(K key) {
        bounds[0] = new Bound<>(key, null, false);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> gteK(K key) {
        bounds[0] = new Bound<>(key, null, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> ltK(K key) {
        bounds[1] = new Bound<>(key, null, false);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> lteK(K key) {
        bounds[1] = new Bound<>(key, null, true);
        return this;
    }

    @Override
    public IndexQuery<K, V, I> limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public IndexQuery<K, V, I> offset(int offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public IndexQuery<K, V, I> descending(boolean descending) {
        this.descending = descending;
        return this;
    }

    @Override
    public IReadCommand<V> first() {
        return readTxn -> readTxn.singleGetIndex(descending(false).limit(1));
    }

    @Override
    public IReadCommand<V> next(V value) {
        bounds[0] = new Bound<>(indexDef.entity().keyOf(value), indexDef.getter().apply(value), false);
        return readTxn -> readTxn.singleGetIndex(descending(false).limit(1));
    }

    @Override
    public IReadCommand<V> last() {
        return readTxn -> readTxn.singleGetIndex(descending(true).limit(1));
    }

    @Override
    public IReadCommand<V> prev(V value) {
        bounds[1] = new Bound<>(indexDef.entity().keyOf(value), indexDef.getter().apply(value), false);
        return readTxn -> readTxn.singleGetIndex(descending(true).limit(1));
    }

    @Override
    public IBatchGet<K, V> get() {
        return new IBatchGet<K, V>() {
            @Override
            public IReadCommand<List<K>> asKeyList() {
                return readTxn -> readTxn.batchGetIndexKeys(IndexQuery.this);
            }

            @Override
            public IReadCommand<List<V>> asValueList() {
                return readTxn -> readTxn.batchGetIndexValues(IndexQuery.this);
            }

            @Override
            public IReadCommand<Map<K, V>> asMap() {
                return readTxn -> readTxn.batchGetIndexMap(IndexQuery.this);
            }
        };
    }

}
