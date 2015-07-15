package com.zakgof.db.velvet.api.entity;

import com.zakgof.db.velvet.VelvetUtil;

public class Entity<K, V> implements IEntityDef<K, V> {

  private Class<V> valueClass;
  private Class<K> keyClass;
  private String kind;

  public static <K, V> Entity<K, V> of(Class<V> valueClass) {
    return new Entity<>(valueClass);
  }

  @SuppressWarnings("unchecked")
  public Entity(Class<V> valueClass) {
    this.valueClass = valueClass;
    this.keyClass = (Class<K>) VelvetUtil.keyClassOf(valueClass);
    this.kind = VelvetUtil.kindOf(valueClass);
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
    return keyClass.cast(VelvetUtil.keyOf(value));
  }

  @Override
  public String getKind() {
    return kind;
  }

}
