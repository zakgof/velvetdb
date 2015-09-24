package com.zakgof.db.velvet.impl.entity;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.annotation.Key;
import com.zakgof.db.velvet.annotation.SortedKey;
import com.zakgof.tools.generic.Functions;

public class AnnoKeyProvider<K, V> implements Function<V, K> {

  private final Function<V, K> provider;
  private final Class<K> keyClass;
  private boolean sorted;

  @SuppressWarnings("unchecked")
  public AnnoKeyProvider(Class<V> valueClass) {
    for (Field field : getAllFields(valueClass)) {
      field.setAccessible(true);
      if (field.getAnnotation(Key.class) != null || field.getAnnotation(SortedKey.class) != null) {
        keyClass = (Class<K>) field.getType();
        sorted = field.getAnnotation(SortedKey.class) != null;
        provider = value -> {
          try {
            return (K) field.get(value);
          } catch (IllegalAccessException e) {
            throw new VelvetException(e);
          }
        };
        return;
      }
    }
    for (Method method : valueClass.getDeclaredMethods()) { // TODO: include inherited
      method.setAccessible(true);
      if (method.getAnnotation(Key.class) != null || method.getAnnotation(SortedKey.class) != null) {
        keyClass = (Class<K>) method.getReturnType();
        sorted = method.getAnnotation(SortedKey.class) != null;
        provider = (value -> {
          try {
            return (K) method.invoke(value);
          } catch (IllegalAccessException | InvocationTargetException e) {
            throw new VelvetException(e);
          }
        });
        return;
      }
    }
    throw new VelvetException("No annotation for key found in " + valueClass);
  }

  static List<Field> getAllFields(Class<?> type) {
    List<Field> fields = new ArrayList<Field>();
    Field[] declaredFields = type.getDeclaredFields();
    Arrays.sort(declaredFields, Functions.comparator(Field::getName));
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
  
  public boolean isSorted() {
    return sorted;
  }

}