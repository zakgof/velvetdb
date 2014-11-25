package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import com.zakgof.db.ILockable;
import com.zakgof.db.ITransactional;

public interface IRawVelvet extends ITransactional, ILockable {

  public <T> T get(Class<T> clazz, String kind, Object key);
  public <K> Collection<K> allKeys(String kind, Class<K> keyClass);
  
  public <T> void put(String kind, Object key, T value);
  public void delete(String kind, Object key);
  
  public ILink index(Object key1, String edgekind, LinkType type);
  public <T> ISortedIndexLink index(Object key1, String edgekind, Comparator<T> comparator, Class<T> clazz, String kind);
  
  interface ILink {
    void connect(Object key2);   
    void disconnect(Object key2);
    <K> List<K> linkKeys(Class<K> clazz);
  }
  
  interface ISortedIndexLink extends ILink {
    void update(Object key2);
    <K> List<K> linkKeys(Class<K> clazz, IndexQuery<K, ?> query);
  }
  
  enum LinkType {
    Single,
    Multi;
  }
  
  /**
   * 
   * VELVET
   * RAW VELVET
   * INDEXER
   * KVS
   * DB
   * */

  

}


