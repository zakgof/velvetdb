package com.zakgof.db.sqlkvs;

import java.util.HashMap;
import java.util.Map;

import com.zakgof.db.kvs.ITransactionalKvs;

public class MemKvs implements ITransactionalKvs {
  
  private final Map<Object, Object> values = new HashMap<>();

  @Override
  public <T> T get(Class<T> clazz, Object key) {
    Object object = values.get(key);
    return (T)object;
  }

  @Override
  public <T> void put(Object key, T value) {
    values.put(key, value);
  }

  @Override
  public void delete(Object key) {
    values.remove(key);
  }

  @Override
  public void rollback() {
    // Not implemented
  }

  @Override
  public void commit() {
    // TODO Auto-generated method stub
  }

}
