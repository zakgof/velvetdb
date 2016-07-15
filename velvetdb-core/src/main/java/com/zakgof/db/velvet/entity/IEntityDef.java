package com.zakgof.db.velvet.entity;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStoreIndex;
import com.zakgof.db.velvet.properties.IPropertyAccessor;

public interface IEntityDef<K, V> {

  public Class<K> getKeyClass();

  public Class<V> getValueClass();

  public K keyOf(V value);

  public String getKind();
  
  //

  public V get(IVelvet velvet, K key);

  public List<K> keys(IVelvet velvet);

  public long size(IVelvet velvet);

  public void put(IVelvet velvet, V value);

  public boolean containsKey(IVelvet velvet, K key);

  public void deleteKey(IVelvet velvet, K key);

  public IPropertyAccessor<K, V> propertyAccessor();

  public <M extends Comparable<? super M>> IStoreIndex<K, M> index(IVelvet velvet, String name);

  public default List<V> get(IVelvet velvet, Collection<K> keys) {
    return keys.stream().map(key -> get(velvet, key)).collect(Collectors.toList());
  }

  public default List<V> get(IVelvet velvet) {
    return get(velvet, keys(velvet));
  }

  public default void deleteValue(IVelvet velvet, V value) {
    deleteKey(velvet, keyOf(value));
  }

  public default boolean equals(V value1, V value2) {
    return keyOf(value1).equals(keyOf(value2));
  }

}