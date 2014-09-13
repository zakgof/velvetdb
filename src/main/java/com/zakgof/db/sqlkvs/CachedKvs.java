package com.zakgof.db.sqlkvs;

import com.zakgof.db.ICache;
import com.zakgof.db.kvs.ITransactionalKvs;

public class CachedKvs implements ITransactionalKvs {

  private final ITransactionalKvs kvs;
  private final ICache cache;

  public CachedKvs(ITransactionalKvs kvs, ICache cache) {
    this.kvs = kvs;
    this.cache = cache;
  }

  @Override
  public <T> T get(Class<T> clazz, Object key) {
    T cached = clazz.cast(cache.get(key));
    if (cached != null)
      return cached;
    
    T value = kvs.get(clazz, key);
    cache.put(key, value);
    
    return value;
  }

  @Override
  public <T> void put(Object key, T value) {
    cache.put(key, value);
    kvs.put(key, value);
  }

  @Override
  public void delete(Object key) {
    cache.remove(key);
    kvs.delete(key);
  }

  @Override
  public void rollback() {
    kvs.rollback();
  }

  @Override
  public void commit() {
    kvs.commit();
  }

}
