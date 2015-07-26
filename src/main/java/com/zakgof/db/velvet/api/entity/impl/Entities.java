package com.zakgof.db.velvet.api.entity.impl;

import java.util.Locale;

import com.zakgof.db.velvet.annotation.Kind;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public enum Entities {
  ;
  
  public static <K, V> IEntityDef<K, V>  create(Class<V> valueClass) {
    AnnoKeyProvider<K, V> annoKeyProvider = new AnnoKeyProvider<K, V>(valueClass);
    Class<K> keyClass = annoKeyProvider.getKeyClass();
    String kind = kindOf(valueClass);
    return new Entity<>(keyClass, valueClass, kind, annoKeyProvider);
  }

  public static String kindOf(Class<?> clazz) {
    Kind annotation = clazz.getAnnotation(Kind.class);
    if (annotation != null)
      return annotation.value();
    String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    return kind;
  }

}
