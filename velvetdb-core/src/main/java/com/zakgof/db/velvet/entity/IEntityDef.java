package com.zakgof.db.velvet.entity;

import java.util.Collection;
import java.util.List;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.IRangeQuery;

public interface IEntityDef<K, V> {

    public Class<K> getKeyClass();

    public Class<V> getValueClass();

    public K keyOf(V value);

    public String getKind();

    //

    public V get(IVelvet velvet, K key);

    public List<K> keys(IVelvet velvet);

    public long size(IVelvet velvet);

    public K put(IVelvet velvet, V value);

    public K put(IVelvet velvet, K key, V value);

    public boolean containsKey(IVelvet velvet, K key);

    public void deleteKey(IVelvet velvet, K key);

    public IPropertyAccessor<K, V> propertyAccessor();

    //
    // public <M extends Comparable<? super M>> IStoreIndex<K, M> index(IVelvet velvet, String name);

    // TODO
    public <M extends Comparable<? super M>> List<V> index(IVelvet velvet, String indexName, IRangeQuery<K, M> query);
    public <M extends Comparable<? super M>> List<K> indexKeys(IVelvet velvet, String indexName, IRangeQuery<K, M> query);

    public List<V> get(IVelvet velvet, Collection<K> keys);

    public List<V> get(IVelvet velvet);

    public void deleteValue(IVelvet velvet, V value);

    public boolean equals(V value1, V value2);

}
