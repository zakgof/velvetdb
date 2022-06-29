package com.zakgof.velvet.request;

import java.util.Collection;

public interface IEntityPut<K, V> {
    IWriteRequest value(V value);
    IWriteRequest values(Collection<V> values);
}
