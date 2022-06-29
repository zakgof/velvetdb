package com.zakgof.velvet.request;

import java.util.Collection;

public interface IEntityDelete<K, V> {
    IWriteRequest key(K key);
    IWriteRequest keys(Collection<K> keys);
    IWriteRequest value(V value);
    IWriteRequest values(Collection<V> values);
}
