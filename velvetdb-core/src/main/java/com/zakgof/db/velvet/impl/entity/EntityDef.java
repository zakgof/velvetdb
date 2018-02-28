package com.zakgof.db.velvet.impl.entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStore;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

public class EntityDef<K, V> implements IEntityDef<K, V> {

    private final Class<V> valueClass;
    private final Class<K> keyClass;
    private final String kind;
    private Function<V, K> keyProvider;
    protected final Collection<IStoreIndexDef<?, V>> indexes;

    public EntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider, Collection<IStoreIndexDef<?, V>> indexes) {
        this(keyClass, valueClass, kind, indexes);
        this.keyProvider = keyProvider;
    }

    public EntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
        this(keyClass, valueClass, kind, keyProvider, Collections.emptyList());
    }

    public EntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Collection<IStoreIndexDef<?, V>> indexes) {
        this.valueClass = valueClass;
        this.keyClass = keyClass;
        this.kind = kind;
        this.indexes = indexes;
    }

    protected void setKeyProvider(Function<V, K> keyProvider) {
        this.keyProvider = keyProvider;
    }

    public IStore<K, V> store(IVelvet velvet) {
        return velvet.store(getKind(), getKeyClass(), getValueClass(), indexes);
    }

    @Override
    public Class<K> getKeyClass() {
        return keyClass;
    }

    @Override
    public Class<V> getValueClass() {
        return valueClass;
    }

    @Override
    public K keyOf(V value) {
        return keyProvider.apply(value);
    }

    @Override
    public String getKind() {
        return kind;
    }

    @Override
    public V get(IVelvet velvet, K key) {
        return store(velvet).get(key);
    }

    @Override
    public Map<K, V> batchGet(IVelvet velvet, List<K> keys) {
        return store(velvet).batchGet(keys);
    }

    @Override
    public Map<K, V> batchGetAll(IVelvet velvet) {
        return store(velvet).getAll();
    }

    @Override
    public List<K> keys(IVelvet velvet) {
        return store(velvet).keys();
    }

    @Override
    public long size(IVelvet velvet) {
        return store(velvet).size();
    }

    @Override
    public K put(IVelvet velvet, V value) {
        K key = keyOf(value);
        return put(velvet, key, value);
    }

    @Override
    public K put(IVelvet velvet, K key, V value) {
        store(velvet).put(key, value);
        return key;
    }

    @Override
    public List<K> put(IVelvet velvet, List<V> values) {
        List<K> keys = values.stream().map(this::keyOf).collect(Collectors.toList());
        return put(velvet, keys, values);
    }

    @Override
    public List<K> put(IVelvet velvet, List<K> keys, List<V> values) {
        store(velvet).put(keys, values);
        return keys;
    }

    @Override
    public void deleteKey(IVelvet velvet, K key) {
        store(velvet).delete(key);
    }

    @Override
    public void deleteKeys(IVelvet velvet, List<K> keys) {
        store(velvet).delete(keys);
    }

    @Override
    public boolean containsKey(IVelvet velvet, K key) {
        return store(velvet).contains(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IPropertyAccessor<K, V> propertyAccessor() {
        if (keyProvider instanceof IPropertyAccessor)
            return (IPropertyAccessor<K, V>) keyProvider;
        return null;
    }

    @Override
    public <M extends Comparable<? super M>> V singleIndex(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query) {
        K key = indexKey(velvet, indexName, query);
        return key == null ? null : get(velvet, key);
    }

    @Override
    public <M extends Comparable<? super M>> K indexKey(IVelvet velvet, String indexName, ISingleReturnSecQuery<K, M> query) {
        List<K> keys = store(velvet).<M> index(indexName).keys((ISecQuery<K, M>)query);
        return keys.isEmpty() ? null : keys.get(0);
    }

    @Override
    public <M extends Comparable<? super M>> List<V> index(IVelvet velvet, String indexName, ISecQuery<K, M> query) {
        List<K> keys = indexKeys(velvet, indexName, query);
        return new ArrayList<>(batchGet(velvet, keys).values());
    }

    @Override
    public <M extends Comparable<? super M>> List<K> indexKeys(IVelvet velvet, String indexName, ISecQuery<K, M> query) {
        return store(velvet).<M> index(indexName).keys(query);
    }

    @Override
    public String toString() {
        return "[" + kind + "]";
    }
}
