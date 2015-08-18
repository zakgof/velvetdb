package com.zakgof.db.velvet.api.entity.impl;

import java.util.Locale;
import java.util.function.Function;

import com.zakgof.db.velvet.annotation.Kind;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public enum Entities {
  ;
  
  public static <K, V> IEntityDef<K, V>  create(Class<V> valueClass) {
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    Class<K> keyClass = annoKeyProvider.getKeyClass();
    String kind = kindOf(valueClass);
    return new EntityDef<>(keyClass, valueClass, kind, annoKeyProvider);
  }
  
  public static <K, V> IEntityDef<K, V>  create(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new EntityDef<>(keyClass, valueClass, kind, keyProvider);
  }
  
  public static <K> IEntityDef<K, K>  selfKeyed(Class<K> clazz, String kind) {
    return new SelfKeyedEntityDef<K>(clazz, kind);
  }

  public static String kindOf(Class<?> clazz) {
    Kind annotation = clazz.getAnnotation(Kind.class);
    if (annotation != null)
      return annotation.value();
    String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    return kind;
  }

}
