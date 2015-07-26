package com.zakgof.db.velvet.api.entity;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;

public interface IEntityDef<K, V> {

  public Class<K> getKeyClass();

  public Class<V> getValueClass();

  public K keyOf(V value);

  public String getKind();

  public default V get(IVelvet velvet, K key) {
    return velvet.get(getValueClass(), getKind(), key);
  }

  public default List<V> getAll(IVelvet velvet, Collection<K> keys) {
    return keys.stream().map(key -> get(velvet, key)).collect(Collectors.toList());
  }
  
  public default List<V> getAll(IVelvet velvet) {
    return getAll(velvet, velvet.allKeys(getKind(), getKeyClass()));
  }
  
  public default List<K> getAllKeys(IVelvet velvet) {
    return velvet.allKeys(getKind(), getKeyClass());
  }

  public default void put(IVelvet velvet, V value) {
    velvet.put(getKind(), keyOf(value), value);
  }

  public default void deleteValue(IVelvet velvet, V value) {
    velvet.delete(getKind(), keyOf(value));
  }
  
  public default void deleteKey(IVelvet velvet, K key) {
    velvet.delete(getKind(), key);
  }
}
