package com.zakgof.velvet.entity;

import com.zakgof.velvet.properties.IPropertyAccessor;
import com.zakgof.velvet.request.*;

import java.util.Set;

public interface IEntityDef<K, V> {

    Class<K> keyClass();
    Class<V> valueClass();
    String kind();
    boolean sorted();

    IPropertyAccessor<K, V> propertyAccessor();
    K keyOf(V value);

    <I> IIndexDef<K, V, I> index(String name);
    Set<String> indexes();

    IEntityGet<K, V> get();
    IEntityPut<V> put();
    IEntityDelete<K, V> delete();
}
