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

  public ILink index(Object key1, String edgekind, LinkType type);

  public <T, M extends Comparable<M>> ISortedIndexLink index(Object key1, String edgekind, Class<T> nodeClazz, String nodekind, Function<T, M> nodeMetric);

  public interface ILink {
    <K> void connect(K key2);

    <K> void disconnect(K key2);

    <K> List<K> linkKeys(Class<K> clazz);
  }

  public interface ISortedIndexLink extends ILink {
    void update(Object key2);
    List<?> linkKeys(Class<?> clazz, IndexQuery<?, ?> query);
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
