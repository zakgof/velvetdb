package com.zakgof.db.velvet.impl.entity;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.annotation.AutoKey;
import com.zakgof.db.velvet.annotation.Key;

public class AnnoKeyProvider<K, V> implements Function<V, K> {
  
  private Function<V, K> provider = null;
  private Class<K> keyClass;
  
  @SuppressWarnings("unchecked")
  public AnnoKeyProvider(Class<V> valueClass) {     
    try {
      for (Field field : getAllFields(valueClass)) {
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
        if (method.getAnnotation(Key.class) != null) {
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
      }
      throw new RuntimeException("No annotation for key found in " + valueClass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  static List<Field> getAllFields(Class<?> type) {
    List<Field> fields = new ArrayList<Field>();

    Field[] declaredFields = type.getDeclaredFields();
    Arrays.sort(declaredFields, new Comparator<Field>() {
      @Override
      public int compare(Field f1, Field f2) {
        return f1.getName().compareTo(f2.getName());
      }
    });
    for (Field field : declaredFields)
      fields.add(field);
    if (type.getSuperclass() != null)
      fields.addAll(getAllFields(type.getSuperclass()));
    return fields;
  }

  @Override
  public K apply(V value) {
    return provider.apply(value);
  }

  Class<K> getKeyClass() {
    return keyClass;
  } 
  
}