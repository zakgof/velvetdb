package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.ILockable;
import com.zakgof.db.ITransactional;
import com.zakgof.db.velvet.api.query.IIndexQuery;

public interface IRawVelvet extends ITransactional, ILockable {

  public <T> T get(Class<T> clazz, String kind, Object key);

  public <K> Collection<K> allKeys(String kind, Class<K> keyClass);

  public <T> void put(String kind, Object key, T value);

  public void delete(String kind, Object key);

  public <K> ILink<K> index(Object key1, String edgekind, LinkType type);

  public <K extends Comparable<K>, T> IKeyIndexLink<K> index(Object key1, String edgekind);
  
  public <K, T, M extends Comparable<M>> IKeyIndexLink<K> index(Object key1, String edgekind, Class<T> nodeClazz, String nodekind, Function<T, M> nodeMetric);

  public interface ILink<K> {
    void connect(K key2);
    void disconnect(K key2);
    List<K> linkKeys(Class<K> clazz);
    boolean isConnected(K bkey);
  }
  
  public interface IKeyIndexLink<K> extends ILink<K> {
    void update(K key2);
    List<K> linkKeys(Class<K> clazz, IIndexQuery<K> query);
  }
  
  enum LinkType {
    Single,
    Multi,
    
    PriBTree,
    SecBTree,
  }

  /**
   * 
   * VELVET RAW VELVET INDEXER KVS DB
   * */

}
