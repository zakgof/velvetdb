package com.zakgof.velvet.request;

import java.util.Collection;

public interface IEntityGet<K, V> {

    IReadCommand<V> key(K key);

    IBatchGet<K, V> keys(Collection<K> keys);

    IBatchGet<K, V> all();
}
