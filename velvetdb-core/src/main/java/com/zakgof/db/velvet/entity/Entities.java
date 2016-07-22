package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet.IStoreIndexDef;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.impl.entity.AnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.AnnoKeyProvider;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.impl.entity.KeylessEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedAnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedEntityDef;

final public class Entities {

    public static <K, V> IEntityDef<K, V> create(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
        return new EntityDef<>(keyClass, valueClass, kind, keyProvider);
    }

    public static <K, V> IEntityDef<K, V> create(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider, Collection<IStoreIndexDef<?, V>> indexes) {
        return new EntityDef<>(keyClass, valueClass, kind, keyProvider, indexes);
    }

    public static <K extends Comparable<? super K>, V> ISortableEntityDef<K, V> sorted(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider, Collection<IStoreIndexDef<?, V>> indexes) {
        return new SortedEntityDef<>(keyClass, valueClass, kind, keyProvider, indexes);
    }

    public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass, String kind, Collection<IStoreIndexDef<?, V>> indexes) {
        return new KeylessEntityDef<V>(valueClass, kind, indexes);
    }

    public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass, String kind) {
        return new KeylessEntityDef<V>(valueClass, kind, Collections.emptyList());
    }

    @SafeVarargs
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K, V> IEntityDef<K, V> create(Class<V> valueClass, IStoreIndexDef<?, V>... indexes) {
        AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
        if (!annoKeyProvider.hasKey())
            return (IEntityDef<K, V>) new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass),
                    Arrays.asList(indexes));
        else if (annoKeyProvider.isSorted())
            return (IEntityDef<K, V>) new SortedAnnoEntityDef(valueClass, annoKeyProvider, Arrays.asList(indexes));
        else
            return new AnnoEntityDef<>(valueClass, annoKeyProvider, Arrays.asList(indexes));
    }

    @SuppressWarnings("unchecked")
    @SafeVarargs
    public static <K extends Comparable<K>, V> ISortableEntityDef<K, V> sorted(Class<V> valueClass, IStoreIndexDef<?, V>... indexes) {
        AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
        if (!annoKeyProvider.hasKey()) {
            return (ISortableEntityDef<K, V>) new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass), Arrays.asList(indexes));
        } else if (annoKeyProvider.isSorted()) {
            return new SortedAnnoEntityDef<>(valueClass, annoKeyProvider, Arrays.asList(indexes));
        } else {
            throw new VelvetException("Key not sorted, use @SortedKey");
        }
    }

    @SafeVarargs
    public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass, IStoreIndexDef<?, V>... indexes) {
        return new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass), Arrays.asList(indexes));
    }

    public static <V> Builder<V> from(Class<V> clazz) {
        return new Builder<V>(clazz);
    }

    public static class Builder<V> {

        private Class<V> clazz;
        private List<IStoreIndexDef<?, V>> indexes = new ArrayList<>();

        private Builder (Class<V> clazz) {
            this.clazz = clazz;
        }

        public <M extends Comparable<? super M>> Builder<V> index(String name, Function<V, M> metric) {
            indexes.add(Indexes.create(name, metric));
            return this;
        }

        public  <K> IEntityDef<K, V> make(Class<K> keyClass, Function<V, K> keyFunction) {
            String kind = AnnoEntityDef.kindOf(clazz);
            return new EntityDef<>(keyClass, clazz, kind, keyFunction, indexes);
        }

        public <K> IEntityDef<K, V> make() {
            AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(clazz);
            return new AnnoEntityDef<>(clazz, annoKeyProvider, indexes);
        }

        public IKeylessEntityDef<V> makeKeyless(String kind) {
            return new KeylessEntityDef<>(clazz, kind, indexes);
        }

        public IKeylessEntityDef<V> makeKeyless() {
            String kind = AnnoEntityDef.kindOf(clazz);
            return new KeylessEntityDef<>(clazz, kind, indexes);
        }

        public <K extends Comparable<? super K>> ISortableEntityDef<K, V> makeSorted() {
            AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(clazz);
            return new SortedAnnoEntityDef<K, V>(clazz, annoKeyProvider, indexes);
        }

    }
}
