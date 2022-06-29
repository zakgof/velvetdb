package com.zakgof.velvet.request;

import java.util.Collection;

public interface IEntityGet<K, V> {
    IReadRequest<V> key(K key);
    IBatchEntityGet<K, V> keys(Collection<K> keys);
    IBatchEntityGet<K, V> all();
}
