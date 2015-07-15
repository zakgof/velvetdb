package com.zakgof.db.velvet.api.entity;

import java.util.List;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;

public interface IEntityDef<K, V> {

  public Class<K> getKeyClass();

  public Class<V> getValueClass();

  public K keyOf(V value);

  public String getKind();

  public default V get(IVelvet velvet, K key) {
    return velvet.get(getValueClass(), key);
  }

  public default List<V> getAll(IVelvet velvet, List<K> keys) {
    return keys.stream().map(key -> get(velvet, key)).collect(Collectors.toList());
  }

  public default void put(IVelvet velvet, V value) {
    velvet.put(value);
  }
}
