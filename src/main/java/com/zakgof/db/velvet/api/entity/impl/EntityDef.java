package com.zakgof.db.velvet.api.entity.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.annotation.AutoKey;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class EntityDef<K, V> implements IEntityDef<K, V> {

  private final Class<V> valueClass;
  private final Class<K> keyClass;
  private final String kind;
  private final Function<V, K> keyProvider;

  EntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
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
    return keyProvider.apply(value);
  }

  @Override
  public String getKind() {
    return kind;
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
  public void put(IVelvet velvet, V value) {
    K key = keyOf(value);
    if (key == null && keyClass == Long.class) {
      key = genZeroAutoKey(velvet, value);
    }
    velvet.put(getKind(), key, value);
  }
  
  // TODO
  private K genZeroAutoKey(IVelvet velvet, V node) {
    try {
      Class<?> clazz = node.getClass();
      for (Field field : getAllFields(clazz)) {
        field.setAccessible(true);
        if (field.getAnnotation(AutoKey.class) != null) {
          if (field.get(node) == null) {
            Long id = genUniqueId(velvet);
            K kid = keyClass.cast(id);
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
      if (velvet.get(valueClass, kind, key) == null)
        return key;
    }
  }

  

}
