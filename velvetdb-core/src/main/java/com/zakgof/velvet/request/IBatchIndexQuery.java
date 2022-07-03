package com.zakgof.velvet.request;

public interface IBatchIndexQuery<K, V, I> {

    IBatchIndexQuery<K, V, I> eq(I value);
    IBatchIndexQuery<K, V, I> gt(I value);
    IBatchIndexQuery<K, V, I> gte(I value);
    IBatchIndexQuery<K, V, I> lt(I value);
    IBatchIndexQuery<K, V, I> lte(I value);
    IBatchIndexQuery<K, V, I> limit(int limit);
    IBatchIndexQuery<K, V, I> offset(int offset);
    IBatchIndexQuery<K, V, I> descending(boolean descending);

    IReadRequest<V> first();
    IReadRequest<V> next(V value);
    IReadRequest<V> last();
    IReadRequest<V> prev(V value);

    IBatchGet<K, V> get();
}
