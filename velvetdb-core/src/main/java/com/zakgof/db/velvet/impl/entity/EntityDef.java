package com.zakgof.db.velvet.impl.entity;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.annimon.stream.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStore;
import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.entity.AEntityDef;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

public class EntityDef<K, V> extends AEntityDef<K, V> implements IEntityDef<K, V> {

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
    public byte[] getRaw(IVelvet velvet, K key) {
        return store(velvet).getRaw(key);
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
        return put(velvet, keyOf(value), value);
    }

    @Override
    public K put(IVelvet velvet, K key, V value) {
        store(velvet).put(key, value);
        return key;
    }

    @Override
    public void deleteKey(IVelvet velvet, K key) {
        store(velvet).delete(key);
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
    public <M extends Comparable<? super M>> V singleIndex(IVelvet velvet, String indexName, ISingleReturnRangeQuery<K, M> query) {
        K key = indexKey(velvet, indexName, query);
        return key == null ? null : get(velvet, key);
    }

    @Override
    public <M extends Comparable<? super M>> K indexKey(IVelvet velvet, String indexName, ISingleReturnRangeQuery<K, M> query) {
        List<K> keys = store(velvet).<M> index(indexName).keys(query);
        return keys.isEmpty() ? null : keys.get(0);
    }

    @Override
    public <M extends Comparable<? super M>> List<V> index(IVelvet velvet, String indexName, IRangeQuery<K, M> query) {
        List<K> keys = indexKeys(velvet, indexName, query);
        return get(velvet, keys);
    }

    @Override
    public <M extends Comparable<? super M>> List<K> indexKeys(IVelvet velvet, String indexName, IRangeQuery<K, M> query) {
        return store(velvet).<M> index(indexName).keys(query);
    }

    @Override
    public String toString() {
        return "[" + kind + "]";
    }
}
