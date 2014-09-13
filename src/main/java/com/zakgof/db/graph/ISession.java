package com.zakgof.db.graph;

import java.util.List;

import com.zakgof.db.ILockable;
import com.zakgof.db.ITransactional;

public interface ISession extends ITransactional, ILockable {

  public <T> T get(Class<T> clazz, String kind, Object id);
  public <T> List<T> allOf(Class<T> clazz, String kind);
  public void put(String kind, Object key, Object value);
  public void delete(String kind, Object key);

  public <T> List<T> links(Class<T> clazz, Object key, String edgekind, String kind);
  public <K> List<K> linkKeys(Class<K> clazz, Object key, String edgekind);

  public void connect(Object key1, Object key2, String edgekind);
  public void disconnect(Object key1, Object key2, String edgekind);

}
