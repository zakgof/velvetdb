package com.zakgof.db.velvet.api.entity.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Function;

import com.zakgof.db.velvet.annotation.AutoKey;
import com.zakgof.db.velvet.annotation.Key;

class AnnoKeyProvider<K, V> implements Function<V, K> {
  
  private Function<V, K> provider = null;
  private Class<K> keyClass;
  
  @SuppressWarnings("unchecked")
  AnnoKeyProvider(Class<V> valueClass) {     
    try {
      for (Field field : Entity.getAllFields(valueClass)) {
        field.setAccessible(true);
        if (field.getAnnotation(Key.class) != null || field.getAnnotation(AutoKey.class) != null) {
          keyClass = (Class<K>) field.getType();
          provider = value -> {
            try {
              return (K)field.get(value);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };
          return;
        }
      }
      for (Method method : valueClass.getDeclaredMethods()) { // TODO: include inherited
        method.setAccessible(true);
        if (method.getAnnotation(Key.class) != null)
          keyClass = (Class<K>) method.getReturnType();
          provider = (value -> {
            try {
              return (K) method.invoke(value);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
        return;
      }
      throw new RuntimeException("No annotation for key found in " + valueClass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public K apply(V value) {
    return provider.apply(value);
  }

  Class<K> getKeyClass() {
    return keyClass;
  } 
  
}