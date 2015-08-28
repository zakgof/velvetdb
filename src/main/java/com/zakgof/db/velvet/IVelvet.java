package com.zakgof.db.velvet;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.ILockable;
import com.zakgof.db.ITransactional;
import com.zakgof.db.velvet.api.query.IIndexQuery;

public interface IVelvet extends ITransactional, ILockable {

  public <K, V> IStore<K, V> store(String kind, Class<K> keyClass, Class<V> valueClass);
  
  public <K extends Comparable<K>, V> ISortedStore<K, V> sortedStore(String kind, Class<K> keyClass, Class<V> valueClass);
  
  
  public <K> ILink<K> simpleIndex(Object key1, String edgekind, LinkType type);

  public <K extends Comparable<K>, T> IKeyIndexLink<K, K> primaryKeyIndex(Object key1, String edgekind);
  
  public <K, T, M extends Comparable<M>> IKeyIndexLink<K, M> secondaryKeyIndex(Object key1, String edgekind, Class<T> nodeClazz, String nodekind, Function<T, M> nodeMetric, Class<M> mclazz);
  
  public interface IStore<K, V> {
    V get(K key);
    void put(K key, V value);
    void delete(K key);
    List<K> keys();
    boolean contains(K key);
  }
  
  public interface ISortedStore<K extends Comparable<K>, V> extends IStore<K, V> {
    List<K> keys(IIndexQuery<K> query);
  }

  public interface ILink<K> {
    void put(K key2);
    void delete(K key2);
    List<K> keys(Class<K> clazz);
    boolean contains(K key2);
  }
  
  public interface IKeyIndexLink<K, M extends Comparable<M>> extends ILink<K> {
    void update(K key2);
    List<K> keys(Class<K> clazz, IIndexQuery<M> query);
  }
  
  public enum LinkType {
    Single,
    Multi,
  }
}
