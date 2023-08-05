package com.zakgof.velvet.request;

import java.util.Collection;

public interface IEntityDelete<K, V> {

    IWriteCommand key(K key);

    IWriteCommand keys(Collection<K> keys);

    IWriteCommand value(V value);

    IWriteCommand values(Collection<V> values);
}
