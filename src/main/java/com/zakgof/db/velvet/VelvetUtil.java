package com.zakgof.db.velvet;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import com.zakgof.db.velvet.annotation.AutoKey;
import com.zakgof.db.velvet.annotation.Key;
import com.zakgof.db.velvet.annotation.Kind;
import com.zakgof.tools.generic.IFunction;

public class VelvetUtil {

  public static Object keyOf(Object node) {
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

  public static String keyFieldOf(Class<?> clazz) {
    try {
      for (Field field : getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(Key.class) != null || field.getAnnotation(AutoKey.class) != null)
          return field.getName();
      }
      for (Method method : clazz.getDeclaredMethods()) {
        method.setAccessible(true);
        if (method.getAnnotation(Key.class) != null)
          return method.getName();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Not key field found for " + clazz);
  }

  public static Class<?> keyClassOf(Class<?> clazz) {
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

  public static List<Field> getAllFields(Class<?> type) {
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

  public static String kindOf(Class<?> clazz) {
    Kind annotation = clazz.getAnnotation(Kind.class);
    if (annotation != null)
      return annotation.value();
    String kind = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    return kind;
  }

  public static boolean equals(Object node1, Object node2) {
    return keyOf(node1).equals(keyOf(node2));
  }
  
  public static <T> List<T> getAll(IVelvet velvet, List<?> keys, Class<T> clazz) {
    List<T> nodes = new ArrayList<T>(keys.size());
    for (Object key : keys) {
      T node = velvet.get(clazz, key);
      nodes.add(node);
    }
    return nodes;
  }

  public static <A, B> void upgrade(IVelvet velvet, Class<A> oldClass, Class<B> newClass, IFunction<A, B> convertor) {
    String kind = kindOf(newClass);
    if (!kind.equals(kindOf(oldClass)))
      throw new RuntimeException("Upgrage impossible: kinds not match");
    List<A> oldValues = velvet.allOf(oldClass);
    for (A oldValue : oldValues) {
      B newValue = convertor.get(oldValue);
      if (!keyOf(newValue).equals(keyOf(oldValue)))
        throw new RuntimeException("Upgrage error: keys not match");
      velvet.put(newValue);
    }
  }

  public static boolean isAutoKeyed(Class<?> clazz) {
    try {
      for (Field field : getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(AutoKey.class) != null)
          return true;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  public static boolean isEntity(Object node) {
    try {
      keyOf(node);
    } catch (Throwable e) {
      return false;
    }
    return true;
  }

}
