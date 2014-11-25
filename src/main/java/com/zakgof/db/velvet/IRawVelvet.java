package com.zakgof.db.velvet;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.ILockable;
import com.zakgof.db.ITransactional;

public interface IRawVelvet extends ITransactional, ILockable {

  public <T> T get(Class<T> clazz, String kind, Object key);

  public <K> Collection<K> allKeys(String kind, Class<K> keyClass);

  public <T> void put(String kind, Object key, T value);

  public void delete(String kind, Object key);

  public <K> ILink<K> index(Object key1, String edgekind, LinkType type);

  public <K, T, M extends Comparable<M>> ISortedIndexLink<K, T, M> index(Object key1, String edgekind, Class<T> nodeClazz, String nodekind, Function<T, M> nodeMetric);

  public interface ILink<K> {
    void connect(K key2);
    void disconnect(K key2);
    List<K> linkKeys(Class<K> clazz);
  }

  public interface ISortedIndexLink<K, T, M extends Comparable<M> > extends ILink<K> {
    void update(K key2);
    List<K> linkKeys(Class<K> clazz, IndexQuery<T, M> query);
  }

  enum LinkType {
    Single,
    Multi;
  }

  /**
   * 
   * VELVET RAW VELVET INDEXER KVS DB
   * */

}
