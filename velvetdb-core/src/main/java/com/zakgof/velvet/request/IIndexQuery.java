package com.zakgof.velvet.request;

public interface IIndexQuery<K, V, I> {

    IIndexQuery<K, V, I> eq(I value);
    IIndexQuery<K, V, I> gt(I value);
    IIndexQuery<K, V, I> gte(I value);
    IIndexQuery<K, V, I> lt(I value);
    IIndexQuery<K, V, I> lte(I value);
    IIndexQuery<K, V, I> limit(int limit);
    IIndexQuery<K, V, I> offset(int offset);
    IIndexQuery<K, V, I> descending(boolean descending);

    IIndexQuery<K, V, I> first();
    IIndexQuery<K, V, I> next(V value);
    IIndexQuery<K, V, I> last();
    IIndexQuery<K, V, I> prev(V value);

    IBatchEntityGet<K, V> get();
}
