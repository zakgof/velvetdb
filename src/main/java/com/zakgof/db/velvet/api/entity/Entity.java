package com.zakgof.db.velvet.api.entity;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.annotation.AutoKey;
import com.zakgof.db.velvet.annotation.Key;
import com.zakgof.db.velvet.annotation.Kind;

public class Entity<K, V> implements IEntityDef<K, V> {

  private Class<V> valueClass;
  private Class<K> keyClass;
  private String kind;
  private Function<V, K> keyProvider;

  public static <K, V> Entity<K, V> of(Class<V> annotatedValueClass) {
    return new Entity<>(annotatedValueClass);
  }
  
  public static <K, V> Entity<K, V> of(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    return new Entity<K, V>(keyClass, valueClass, kind, keyProvider);
  }

  @SuppressWarnings("unchecked")
  public Entity(Class<V> valueClass) {
    this.valueClass = valueClass;
    this.keyClass = (Class<K>) keyClassFor(valueClass);
    this.kind = kindFromClass(valueClass);
  }
  
  

  public Entity(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    this.valueClass = valueClass;
    this.keyClass = keyClass;
    this.kind = kind;
    this.keyProvider = keyProvider;
  }

  @Override
  public Class<K> getKeyClass() {
    return keyClass;
  }

  @Override
  public Class<V> getValueClass() {
    return valueClass;
  }

  @Override
  public K keyOf(V value) {
    return keyClass.cast(keyOfValue(value));
  }

  @Override
  public String getKind() {
    return kind;
  }
  
  private static Object keyOfValue(Object node) {
    if (node == null)
      return null;
    try {
      Class<?> clazz = node.getClass();
      for (Field field : getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(Key.class) != null || field.getAnnotation(AutoKey.class) != null)
          return field.get(node);
      }
      for (Method method : clazz.getDeclaredMethods()) {
        method.setAccessible(true);
        if (method.getAnnotation(Key.class) != null)
          return method.invoke(node);
      }
      throw new RuntimeException("No key found for " + clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

//  private static String keyFieldOf(Class<?> clazz) {
//    try {
//      for (Field field : getAllFields(clazz)) {
//        field.setAccessible(true);
//        if (field.getAnnotation(Key.class) != null || field.getAnnotation(AutoKey.class) != null)
//          return field.getName();
//      }
//      for (Method method : clazz.getDeclaredMethods()) {
//        method.setAccessible(true);
//        if (method.getAnnotation(Key.class) != null)
//          return method.getName();
//      }
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//    throw new RuntimeException("Not key field found for " + clazz);
//  }

  private static Class<?> keyClassFor(Class<?> clazz) {
    try {
      for (Field field : getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(Key.class) != null || field.getAnnotation(AutoKey.class) != null)
          return field.getType();
      }
      for (Method method : clazz.getDeclaredMethods()) {
        method.setAccessible(true);
        if (method.getAnnotation(Key.class) != null)
          return method.getReturnType();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("No key field found for " + clazz);
  }

  private static List<Field> getAllFields(Class<?> type) {
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

  public static String kindFromClass(Class<?> clazz) {
    Kind annotation = clazz.getAnnotation(Kind.class);
    if (annotation != null)
      return annotation.value();
    String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    return kind;
  }

  public static boolean equals(Object node1, Object node2) {
    return keyOfValue(node1).equals(keyOfValue(node2));
  }
  
  @Override
  public void put(IVelvet velvet, V value) {
    K key = keyOf(value);
    if (key == null) {
      genZeroAutoKey(velvet, value);
    }
    velvet.put(getKind(), key, value);
  }
  
  private void genZeroAutoKey(IVelvet velvet, V node) {
    try {
      Class<?> clazz = node.getClass();
      for (Field field : getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(AutoKey.class) != null) {
          if (field.get(node) == null) {
            field.set(node, field.getType().cast(genUniqueId(velvet)));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Long genUniqueId(IVelvet velvet) {
    for (;;) {
      Long key = -UUID.randomUUID().getLeastSignificantBits(); // TODO : lock and check
      if (velvet.get(valueClass, kind, key) == null)
        return key;
    }
  }

  

}
