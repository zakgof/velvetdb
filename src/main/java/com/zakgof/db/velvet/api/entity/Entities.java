package com.zakgof.db.velvet.api.entity;

import java.util.function.Function;

import com.zakgof.db.velvet.annotation.Keyless;
import com.zakgof.db.velvet.api.entity.impl.AnnoEntityDef;
import com.zakgof.db.velvet.api.entity.impl.AnnoKeyProvider;
import com.zakgof.db.velvet.api.entity.impl.EntityDef;
import com.zakgof.db.velvet.api.entity.impl.KeylessEntityDef;
import com.zakgof.db.velvet.api.entity.impl.SortedAnnoEntityDef;
import com.zakgof.db.velvet.api.entity.impl.SortedEntityDef;

public enum Entities {
  ;

  @SuppressWarnings("unchecked")
  public static <K, V> IEntityDef<K, V> anno(Class<V> valueClass) {
    if (valueClass.getAnnotation(Keyless.class) != null) {      
      return (IEntityDef<K, V>) new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass));
    }
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

  public static <V> IEntityDef<Long, V> keyless(Class<V> clazz, String kind) {
    return new KeylessEntityDef<V>(clazz, kind);
  }

}
