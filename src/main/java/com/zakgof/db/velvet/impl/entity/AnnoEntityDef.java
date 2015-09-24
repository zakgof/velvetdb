package com.zakgof.db.velvet.impl.entity;

import java.util.Locale;

import com.zakgof.db.velvet.annotation.Kind;

public class AnnoEntityDef<K, V> extends EntityDef<K, V> {
  
  public AnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider) {
    super(annoKeyProvider.getKeyClass(), valueClass, kindOf(valueClass), annoKeyProvider);
  }
   
  public static String kindOf(Class<?> clazz) {
    Kind annotation = clazz.getAnnotation(Kind.class);
    if (annotation != null)
      return annotation.value();
    String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    return kind;
  }


}
