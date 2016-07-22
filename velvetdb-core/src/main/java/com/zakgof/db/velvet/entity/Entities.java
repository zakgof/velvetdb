package com.zakgof.db.velvet.entity;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
}
