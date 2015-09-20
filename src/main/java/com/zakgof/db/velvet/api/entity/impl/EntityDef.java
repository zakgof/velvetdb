package com.zakgof.db.velvet.api.entity.impl;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStore;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class EntityDef<K, V> implements IEntityDef<K, V> {
  
  private final Class<V> valueClass;
  private final Class<K> keyClass;
  private final String kind;
  private Function<V, K> keyProvider;

  public EntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    this(keyClass, valueClass, kind);
    this.keyProvider = keyProvider;
  }
  
  public EntityDef(Class<K> keyClass, Class<V> valueClass, String kind) {
    this.valueClass = valueClass;
    this.keyClass = keyClass;
    this.kind = kind;
  }
  
  
  protected void setKeyProvider(Function<V, K> keyProvider) {
    this.keyProvider = keyProvider;
  }
    
  
  IStore<K, V> store(IVelvet velvet) {
    return velvet.store(getKind(), getKeyClass(), getValueClass());
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
  
  
  public V get(IVelvet velvet, K key) {
    return store(velvet).get(key);
  }
  
  public List<K> keys(IVelvet velvet) {
    return store(velvet).keys();
  }

  public void put(IVelvet velvet, V value) {
    store(velvet).put(keyOf(value), value);
  }
  
  public void deleteKey(IVelvet velvet, K key) {
    store(velvet).delete(key);
  }
  
  

}
