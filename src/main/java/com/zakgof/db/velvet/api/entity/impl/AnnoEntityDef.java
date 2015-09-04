package com.zakgof.db.velvet.api.entity.impl;

import java.lang.reflect.Field;
import java.util.Locale;
import java.util.UUID;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.annotation.AutoKey;
import com.zakgof.db.velvet.annotation.Kind;

public class AnnoEntityDef<K, V> extends EntityDef<K, V> {
  
  public AnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider) {
    super(annoKeyProvider.getKeyClass(), valueClass, kindOf(valueClass), annoKeyProvider);
  }
  
  @Override
  public void put(IVelvet velvet, V value) {
    K key = keyOf(value);
    if (key == null && getKeyClass() == Long.class) {
      key = genZeroAutoKey(velvet, value);
    }
    store(velvet).put(key, value);    
  }
  
  // TODO
  private K genZeroAutoKey(IVelvet velvet, V node) {
    try {
      Class<?> clazz = node.getClass();
      for (Field field : AnnoKeyProvider.getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(AutoKey.class) != null) {
          if (field.get(node) == null) {
            Long id = genUniqueId(velvet);
            K kid = getKeyClass().cast(id);
            field.set(node, kid);
            return kid;
          }
        }
      }
      throw new RuntimeException("Key not found for " + node.getClass());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
   
  }

  private Long genUniqueId(IVelvet velvet) {
    for (;;) {
      Long key = -UUID.randomUUID().getLeastSignificantBits(); // TODO : lock and check
      if (store(velvet).get((K)key) == null)
        return key;
    }
  }
  
  public static String kindOf(Class<?> clazz) {
    Kind annotation = clazz.getAnnotation(Kind.class);
    if (annotation != null)
      return annotation.value();
    String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    return kind;
  }


}
