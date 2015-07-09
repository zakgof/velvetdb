package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public interface IEntityDef<K, V> {
  
  public Class<K> getKeyClass();
  
  public Class<V> getValueClass();
  
  public K keyOf(V value);
  
  public V get(IVelvet velvet, K key);
  
  public List<V> getAll(IVelvet velvet, List<K> keys);
  
  public void put(IVelvet velvet, V value);
  
  public String getKind();
}
