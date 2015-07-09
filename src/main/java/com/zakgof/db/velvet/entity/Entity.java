package com.zakgof.db.velvet.entity;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;
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
  public V get(IVelvet velvet, K key) {
    return velvet.get(valueClass, key);
  }

  @Override
  public void put(IVelvet velvet, V value) {
    velvet.put(value);
  }

  @Override
  public List<V> getAll(IVelvet velvet, List<K> keys) {
    List<V> nodes = new ArrayList<V>(keys.size());
    for (K key : keys) {
      V node = get(velvet, key);
      nodes.add(node);
    }
    return nodes;
  }

  @Override
  public String getKind() {
    return kind;
  }

}
