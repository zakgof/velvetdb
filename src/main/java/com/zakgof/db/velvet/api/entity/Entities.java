package com.zakgof.db.velvet.api.entity;

import java.util.function.Function;

import com.zakgof.db.velvet.api.entity.impl.AnnoEntityDef;
import com.zakgof.db.velvet.api.entity.impl.AnnoKeyProvider;
import com.zakgof.db.velvet.api.entity.impl.EntityDef;
import com.zakgof.db.velvet.api.entity.impl.SortedAnnoEntityDef;
import com.zakgof.db.velvet.api.entity.impl.SortedEntityDef;

public enum Entities {
  ;

  public static <K, V> IEntityDef<K, V> anno(Class<V> valueClass) {
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    return new AnnoEntityDef<>(valueClass, annoKeyProvider);
  }

  public static <K extends Comparable<K>, V> ISortableEntityDef<K, V> sortedAnno(Class<V> valueClass) {
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    return new SortedAnnoEntityDef<>(valueClass, annoKeyProvider);
  }

  public static <K, V> IEntityDef<K, V> create(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new EntityDef<>(keyClass, valueClass, kind, keyProvider);
  }
  
  public static <K extends Comparable<K>, V> ISortableEntityDef<K, V> sorted(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new SortedEntityDef<>(keyClass, valueClass, kind, keyProvider);
  }

  public static <K> IEntityDef<K, K> selfKeyed(Class<K> clazz, String kind) {
    return new EntityDef<>(clazz, clazz, kind, key -> key);
  }

}
