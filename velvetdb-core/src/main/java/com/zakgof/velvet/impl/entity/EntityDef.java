package com.zakgof.velvet.impl.entity;

import com.zakgof.velvet.entity.IEntityDef;
import com.zakgof.velvet.properties.IPropertyAccessor;
import com.zakgof.velvet.request.*;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Accessors(fluent = true)
public class EntityDef<K, V> implements IEntityDef<K, V> {

    @Getter
    private final Class<K> keyClass;

    @Getter
    private final Class<V> valueClass;

    @Getter
    private final String kind;

    @Getter
    private final boolean sorted;

    private final Function<V, K> keyFunction;
    private final Map<String, IIndexDef<K,V,?>> indexes;

    public EntityDef(String kind, Class<V> valueClass, IndexInfo<K, V> key, List<IndexInfo<?, V>> indexes) {
        this(false, kind, valueClass, key, indexes);
    }

    EntityDef(boolean sorted, String kind, Class<V> valueClass, IndexInfo<K, V> key, List<IndexInfo<?, V>> indexes) {
        this.sorted = sorted;
        this.keyClass = key.type();
        this.valueClass = valueClass;
        this.kind = kind;
        this.keyFunction = key.getter();
        this.indexes = indexes.stream()
                .collect(Collectors.toMap(IndexInfo::name, info -> new IndexDef(this, info.name(), info.getter(), info.type())));
    }

    @Override
    public IPropertyAccessor<K, V> propertyAccessor() {
        return null;
    }

    @Override
    public K keyOf(V value) {
        return keyFunction.apply(value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <I> IIndexDef<K, V, I> index(String name) {
        return (IIndexDef)indexes.get(name); // TODO index specific exception
    }

    @Override
    public Set<String> indexes() {
        return indexes.keySet();
    }

    @Override
    public IEntityGet<K, V> get() {
        return new EntityGet<>(this);
    }

    @Override
    public IEntityPut<K, V> put() {
        return new EntityPut<>(this);
    }

    @Override
    public IEntityDelete<K, V> delete() {
        return new EntityDelete<>(this);
    }

    @Override
    public IWriteRequest initialize() {
        return velvet -> velvet.initialize(this);
    }
}
