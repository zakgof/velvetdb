package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.List;

import com.zakgof.db.ILockable;
import com.zakgof.db.ITransactional;

public interface IRawVelvet extends ITransactional, ILockable {

  public <T> T get(Class<T> clazz, String kind, Object id);
  public <K> Collection<K> allKeys(String kind, Class<K> keyClass);
  
  public void put(String kind, Object key, Object value);
  public void delete(String kind, Object key);

  public <T, K> List<T> links(Class<T> clazz, Class<K> keyClazz, Object key, String edgekind, String kind);    
  public <K> List<K> linkKeys(Class<K> clazz, Object key, String edgekind);

  public void connect(Object key1, Object key2, String edgekind);
  public void disconnect(Object key1, Object key2, String edgekind);
  
  
  

}
