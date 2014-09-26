package com.zakgof.db.sqlkvs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.serialize.ZeSerializer;

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

  public void persist(OutputStream stream) throws IOException {
    ZeSerializer serializer = new ZeSerializer();
    serializer.serialize(values, stream);
    stream.flush();
    stream.close();
  }

  public void load(InputStream is) {
    ZeSerializer serializer = new ZeSerializer();
    Map<?, ?> map = serializer.deserialize(is, Map.class);
    this.values.putAll(map);    
  }

}
