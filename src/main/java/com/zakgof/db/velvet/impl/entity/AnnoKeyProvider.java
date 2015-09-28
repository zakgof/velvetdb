package com.zakgof.db.velvet.impl.entity;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.annotation.Key;
import com.zakgof.db.velvet.annotation.SortedKey;
import com.zakgof.db.velvet.properties.IProperty;
import com.zakgof.db.velvet.properties.IPropertyAccessor;
import com.zakgof.tools.generic.Functions;

public class AnnoKeyProvider<K, V> implements Function<V, K>, IPropertyAccessor<K, V> {

  private Function<V, K> provider;
  private Class<K> keyClass;
  private boolean sorted;
  private Map<String, IProperty<?, V>> propMap = new LinkedHashMap<>();
  private IProperty<K, V> keyProp;

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
        keyProp = new FieldProperty<K, V>(field);
      } else {
        propMap.put(field.getName(), new FieldProperty<>(field));
      }
    }
    for (Method method : valueClass.getDeclaredMethods()) { // TODO: include inherited!
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
        keyProp = new MethodProperty<K, V>(method);
      }

      // else {
      // if (method.getParameterCount() == 0 && method.getReturnType() != void.class)
      // propMap.put(method.getName(), new MethodProperty(field));
      // }
    }
    // throw new VelvetException("No annotation for key found in " + valueClass);
  }

  // TODO skip static and transient ?
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

  public boolean hasKey() {
    return provider != null;
  }

  @Override
  public Collection<String> getProperties() {
    return propMap.keySet();
  }

  @Override
  public IProperty<?, V> get(String property) {
    return propMap.get(property);
  }

  @Override
  public IProperty<K, V> getKey() {
    return keyProp;
  }

  private static class FieldProperty<P, V> implements IProperty<P, V> {

    private Field field;

    public FieldProperty(Field field) {
      this.field = field;
    }

    @Override
    public boolean isSettable() {
      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public P get(V instance) {
      try {
        return (P) field.get(instance);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new VelvetException(e);
      }
    }

    @Override
    public void put(V instance, P propValue) {
      try {
        field.set(instance, propValue);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new VelvetException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<P> getType() {
      return (Class<P>) field.getType();
    }

    @Override
    public String getName() {
      return field.getName();
    }

  }

  private static class MethodProperty<P, V> implements IProperty<P, V> {

    private Method method;

    public MethodProperty(Method method) {
      this.method = method;
    }

    @Override
    public boolean isSettable() {
      return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public P get(V instance) {
      try {
        return (P) method.invoke(instance);
      } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
        throw new VelvetException(e);
      }
    }

    @Override
    public void put(V instance, P propValue) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<P> getType() {
      return (Class<P>) method.getReturnType();
    }

    @Override
    public String getName() {
      return method.getName();
    }

  }


}