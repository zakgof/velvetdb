package com.zakgof.db.velvet.entity;

import java.util.List;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public interface IEntityDef<K, V> {

    // Metadata

    public Class<K> getKeyClass();

    public Class<V> getValueClass();

    public String getKind();

    // Query

    public K keyOf(V value);

    public V get(IVelvet velvet, K key);

    public List<V> get(IVelvet velvet, List<K> keys);

    public List<V> getAll(IVelvet velvet);

    public List<K> keys(IVelvet velvet);

    public long size(IVelvet velvet);

    public boolean containsKey(IVelvet velvet, K key);


    // Write

    public K put(IVelvet velvet, V value);

    public K put(IVelvet velvet, K key, V value);

    public List<K> put(IVelvet velvet, List<V> value);

    public List<K> put(IVelvet velvet, List<K> keys, List<V> value);

    // Delete

    public void deleteKey(IVelvet velvet, K key);

    public void deleteKeys(IVelvet velvet, List<K> keys);

    public default void deleteValue(IVelvet velvet, V value) {
        deleteKey(velvet, keyOf(value));
    }

    public default void deleteValues(IVelvet velvet, List<V> values) {
        deleteKeys(velvet, values.stream().map(this::keyOf).collect(Collectors.toList()));
    }

    // Index

    // TODO
    public <M extends Comparable<? super M>> List<V> index(IVelvet velvet, String indexName, IRangeQuery<K, M> query);

    public <M extends Comparable<? super M>> List<K> indexKeys(IVelvet velvet, String indexName, IRangeQuery<K, M> query);

    public <M extends Comparable<? super M>> V singleIndex(IVelvet velvet, String indexName, ISingleReturnRangeQuery<K, M> query);

    public <M extends Comparable<? super M>> K indexKey(IVelvet velvet, String indexName, ISingleReturnRangeQuery<K, M> query);

    // Other

    public IPropertyAccessor<K, V> propertyAccessor();

    public default boolean equals(V value1, V value2) {
        return keyOf(value1).equals(keyOf(value2));
    }

}
