package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

public interface IEntityDef<K, V> {

    // Metadata

    public Class<K> getKeyClass();

    public Class<V> getValueClass();

    public String getKind();

    public IPropertyAccessor<K, V> propertyAccessor();

    // Read

    public K keyOf(V value);

    public V get(IVelvet velvet, K key);

    public Map<K, V> batchGetMap(IVelvet velvet, List<K> keys);

    default public List<V> batchGetList(IVelvet velvet, List<K> keys) {
        return new ArrayList<>(batchGetMap(velvet, keys).values());
    }

    public Map<K, V> batchGetAllMap(IVelvet velvet);

    default public List<V> batchGetAllList(IVelvet velvet) {
        return new ArrayList<>(batchGetAllMap(velvet).values());
    }

    public List<K> batchGetAllKeys(IVelvet velvet);

    public long size(IVelvet velvet);

    public boolean containsKey(IVelvet velvet, K key);


    // Write

    public K put(IVelvet velvet, V value);

    public K put(IVelvet velvet, K key, V value);

    public List<K> batchPut(IVelvet velvet, List<V> value);

    public List<K> batchPut(IVelvet velvet, List<K> keys, List<V> value);

    // Delete

    public void deleteKey(IVelvet velvet, K key);

    public default void deleteValue(IVelvet velvet, V value) {
        deleteKey(velvet, keyOf(value));
    }

    public void batchDeleteKeys(IVelvet velvet, List<K> keys);

    public default void batchDeleteValues(IVelvet velvet, List<V> values) {
        batchDeleteKeys(velvet, values.stream().map(this::keyOf).collect(Collectors.toList()));
    }

    // Index

    // TODO return map
    // TODO rename methods
    public <M extends Comparable<? super M>> List<V> index(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    public <M extends Comparable<? super M>> List<K> indexKeys(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    public <M extends Comparable<? super M>> V singleIndex(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query);

    public <M extends Comparable<? super M>> K indexKey(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query);

    // Other

    public default boolean equals(V value1, V value2) {
        return keyOf(value1).equals(keyOf(value2));
    }

}
