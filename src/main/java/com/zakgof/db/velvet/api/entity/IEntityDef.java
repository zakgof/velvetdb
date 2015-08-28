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
  
  //

  public V get(IVelvet velvet, K key);
  
  public List<K> keys(IVelvet velvet);

  public void put(IVelvet velvet, V value);
  
  public void deleteKey(IVelvet velvet, K key);
  
  
  public default List<V> get(IVelvet velvet, Collection<K> keys) {
    return keys.stream().map(key -> get(velvet, key)).collect(Collectors.toList());
  }
  
  public default List<V> get(IVelvet velvet) {
    return get(velvet, keys(velvet));
  }

  public default void deleteValue(IVelvet velvet, V value) {
    deleteKey(velvet, keyOf(value));
  }
}
