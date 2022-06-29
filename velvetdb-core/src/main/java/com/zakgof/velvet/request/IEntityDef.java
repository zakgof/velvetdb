package com.zakgof.velvet.request;

import com.zakgof.velvet.properties.IPropertyAccessor;

import java.util.Set;

public interface IEntityDef<K, V> {

    public Class<K> keyClass();
    public Class<V> valueClass();
    public String kind();
    public boolean sorted();

    public IPropertyAccessor<K, V> propertyAccessor();
    public K keyOf(V value);

    public <I> IIndexDef<K, V, I> index(String name);
    public Set<String> indexes();


    IEntityGet<K, V> get();
    IEntityPut<K, V> put();
    IEntityDelete<K, V> delete();



}
