package com.zakgof.db.velvet.entity;

import java.util.function.Function;

import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.annotation.Keyless;
import com.zakgof.db.velvet.impl.entity.AnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.AnnoKeyProvider;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.impl.entity.KeylessEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedAnnoEntityDef;
import com.zakgof.db.velvet.impl.entity.SortedEntityDef;

public enum Entities {
  ;

  public static <K, V> IEntityDef<K, V> create(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new EntityDef<>(keyClass, valueClass, kind, keyProvider);
  }

  public static <K extends Comparable<K>, V> ISortableEntityDef<K, V> sorted(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new SortedEntityDef<>(keyClass, valueClass, kind, keyProvider);
  }

  public static <V> IKeylessEntityDef<V> keyless(Class<V> valueClass, String kind) {
    return new KeylessEntityDef<V>(valueClass, kind);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <K, V> IEntityDef<K, V> anno(Class<V> valueClass) {
    if (valueClass.getAnnotation(Keyless.class) != null) {
      return (IEntityDef<K, V>) keylessAnno(valueClass);
    }
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    if (annoKeyProvider.isSorted())
      return (IEntityDef<K, V>) new SortedAnnoEntityDef(valueClass, annoKeyProvider);
    return new AnnoEntityDef<>(valueClass, annoKeyProvider);
  }

  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>, V> ISortableEntityDef<K, V> sortedAnno(Class<V> valueClass) {
    if (valueClass.getAnnotation(Keyless.class) != null) {
      return (ISortableEntityDef<K, V>) keylessAnno(valueClass);
    }
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    if (!annoKeyProvider.isSorted())
      throw new VelvetException("Class used in sortedAnno should have @SortedKey annotation");
    return new SortedAnnoEntityDef<>(valueClass, annoKeyProvider);
  }

  public static <V> IKeylessEntityDef<V> keylessAnno(Class<V> valueClass) {
    return new KeylessEntityDef<V>(valueClass, AnnoEntityDef.kindOf(valueClass));
  }

}
