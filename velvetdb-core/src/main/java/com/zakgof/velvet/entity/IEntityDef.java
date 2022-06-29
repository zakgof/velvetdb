package com.zakgof.velvet.entity;

import com.zakgof.velvet.request.IEntityDelete;
import com.zakgof.velvet.request.IEntityGet;
import com.zakgof.velvet.request.IEntityPut;
import com.zakgof.velvet.request.IIndexDef;

public interface IEntityDef<K, V> {

    public Class<K> keyClass();
    public Class<V> valueClass();
    public String kind();
    public K keyOf(V value);

    public <I> IIndexDef<K, V, I> index(String name);


    IEntityGet<K, V> get();
    IEntityPut<K, V> put();
    IEntityDelete<K, V> delete();
}
