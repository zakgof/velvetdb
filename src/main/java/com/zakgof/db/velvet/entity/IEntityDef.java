package com.zakgof.db.velvet.entity;

import com.zakgof.db.velvet.IVelvet;

public interface IEntityDef<K, V> {
  
  public Class<K> getKeyClass();
  
  public Class<V> getValueClass();
  
  public K keyOf(V value);
  
  public V get(IVelvet velvet, K key);
  
  public void put(IVelvet velvet, V value);
}
