package com.zakgof.db.velvet.entity;

import java.util.function.Function;

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

  public static <K extends Comparable<? super K>, V> ISortableEntityDef<K, V> sorted(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new SortedEntityDef<>(keyClass, valueClass, kind, keyProvider);
  }

  public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass, String kind) {
    return new KeylessEntityDef<V>(valueClass, kind);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <K, V> IEntityDef<K, V> create(Class<V> valueClass) {
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    if (!annoKeyProvider.hasKey())
      return (IEntityDef<K, V>) new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass));
    else if (annoKeyProvider.isSorted())
      return (IEntityDef<K, V>) new SortedAnnoEntityDef(valueClass, annoKeyProvider);
    else
      return new AnnoEntityDef<>(valueClass, annoKeyProvider);
  }

  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>, V> ISortableEntityDef<K, V> sorted(Class<V> valueClass) {
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    if (!annoKeyProvider.hasKey()) {
      return (ISortableEntityDef<K, V>) new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass));
    } else if (annoKeyProvider.isSorted()) {
      return new SortedAnnoEntityDef<>(valueClass, annoKeyProvider);
    } else {
      throw new VelvetException("Key not sorted, use @SortedKey");
    }
  }
  
  public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass) {
    return new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass));
  }

}
