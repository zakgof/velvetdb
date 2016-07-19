package com.zakgof.db.kvs.mapdb;

import java.util.concurrent.ConcurrentNavigableMap;

import com.zakgof.db.kvs.IKvs;

public class MapDbKvs implements IKvs {

  private ConcurrentNavigableMap<Object, Object> map;

  public MapDbKvs(ConcurrentNavigableMap<Object, Object> map) {
    this.map = map;
  }

  public <T> T get(Class<T> clazz, Object key) {
    return clazz.cast(map.get(key));
  }

  public <T> void put(Object key, T value) {
    map.put(key, value);
  }

  public void delete(Object key) {
    map.remove(key);
  }

}
