package com.zakgof.db.velvet.entity;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

public interface IEntityDef<K, V> {

    // Metadata

    public Class<K> getKeyClass();

    public Class<V> getValueClass();

    public String getKind();

    // Query

    public K keyOf(V value);

    public V get(IVelvet velvet, K key);

    public Map<K, V> batchGet(IVelvet velvet, List<K> keys);

    public List<V> batchGetList(IVelvet velvet, List<K> keys);

    public Map<K, V> batchGetAll(IVelvet velvet);

    public List<V> batchGetAllList(IVelvet velvet);

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

    public void deleteValue(IVelvet velvet, V value);

    public void deleteValues(IVelvet velvet, List<V> values);

    // Index

    // TODO
    public <M extends Comparable<? super M>> List<V> index(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    public <M extends Comparable<? super M>> List<K> indexKeys(IVelvet velvet, String indexName, ISecQuery<K, M> query);

    public <M extends Comparable<? super M>> V singleIndex(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query);

    public <M extends Comparable<? super M>> K indexKey(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query);

    // Other

    public IPropertyAccessor<K, V> propertyAccessor();

    public boolean equals(V value1, V value2);

}
