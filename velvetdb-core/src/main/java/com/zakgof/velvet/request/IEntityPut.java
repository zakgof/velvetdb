package com.zakgof.velvet.request;

import java.util.Collection;

public interface IEntityPut<K, V> {
    IWriteCommand value(V value);
    IWriteCommand values(Collection<V> values);
}
