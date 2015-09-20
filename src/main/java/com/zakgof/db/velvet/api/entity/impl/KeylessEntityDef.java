package com.zakgof.db.velvet.api.entity.impl;

import java.util.WeakHashMap;

import com.zakgof.db.velvet.IVelvet;

public class KeylessEntityDef<V> extends SortedEntityDef<Long, V> {
  
  private final WeakHashMap<V, Long> keys = new WeakHashMap<>();
  
  public KeylessEntityDef(Class<V> valueClass, String kind) {
    super(Long.class, valueClass, kind, null);
    setKeyProvider(v -> keys.get(v));
  }
  
  @Override
  public V get(IVelvet velvet, Long key) {
    V value = super.get(velvet, key);    
    keys.put(value, key);
    return value;
  }
  
  @Override
  public void put(IVelvet velvet, V value) {
    Long key = keys.get(value);
    if (key == null) {      
      key = store(velvet).put(value);      
    } else {
      store(velvet).put(key, value);
    }
    keys.put(value, key);
  }
  
  @Override
  public void deleteValue(IVelvet velvet, V value) {
    super.deleteValue(velvet, value);
    keys.remove(value);
  }
  
}
