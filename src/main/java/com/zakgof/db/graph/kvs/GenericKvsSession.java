package com.zakgof.db.graph.kvs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.zakgof.db.graph.ISession;
import com.zakgof.db.graph.KeyGen;
import com.zakgof.db.kvs.ITransactionalKvs;

/**
 * 
 *  Hardcoded keys:
 *  <p>#kind = kind node
 *  <p>&#64;edgeKind + key = edge node
 *  <p>&#64;&#64; = kind connection node
 *
 */
public class GenericKvsSession implements ISession {
  
  private final ITransactionalKvs kvs;
  
  public GenericKvsSession(ITransactionalKvs kvs) {
    this.kvs = kvs;
  }

  @Override
  public <T> T get(Class<T> clazz, String kind, Object key) {
    Object compositeKey = composite(kind, key);
    return kvs.get(clazz, compositeKey);
  }
  
  @Override
  public void put(String kind, Object key, Object value) {
    Object compositeKey = composite(kind, key);
    kvs.put(compositeKey, value);
    connect("#" + kind, key, "@");
  }

  @Override
  public void delete(String kind, Object key) {
    Object compositeKey = composite(kind, key);
    kvs.delete(compositeKey);
    disconnect("#" + kind, key, "@");
  }

  private Object composite(String prefix, Object key) {
    return KeyGen.key(prefix, key);
  }

  @Override
  public <T> List<T> allOf(Class<T> clazz, String kind) {
    return links(clazz, "#" + kind, "@", kind);
  }

  @Override
  public <T> List<T> links(Class<T> clazz, Object key, String edgekind, String kind) {
    List<T> list = new ArrayList<T>();
    Set<Object> linkKeys = getIndex(edgekind, key);
    for (Object linkkey : linkKeys)
      list.add(get(clazz, kind, linkkey));
    return list;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K> List<K> linkKeys(Class<K> clazz, Object key, String edgeKind) {
    Set<Object> index = getIndex(edgeKind, key);
    return new ArrayList<K>((Set<K>)index);
  }
  
  private Set<Object> getIndex(String edgeKind, Object key) {
    Object indexKey = indexKey(edgeKind, key);
    return getIndex(indexKey);
  }

  @Override
  public void connect(Object key1, Object key2, String edgeKind) {
    Object indexKey = indexKey(edgeKind, key1);
    Set<Object> index = getIndex(indexKey);
    HashSet<Object> newIndex = new HashSet<Object>(index);
    newIndex.add(key2);
    kvs.put(indexKey, newIndex);
  }

  private Set<Object> getIndex(Object indexKey) {
    @SuppressWarnings("unchecked")
    Set<Object> index = kvs.get(Set.class, indexKey);
    if (index == null)
      index = new HashSet<Object>();
    return index;
  }

  @Override
  public void disconnect(Object key1, Object key2, String edgeKind) {
    Object indexKey = indexKey(edgeKind, key1);
    Set<Object> index = getIndex(indexKey);
    index.remove(key2);
    if (index.isEmpty())
      kvs.delete(indexKey);
    else
      kvs.put(indexKey, index);
  }

  @Override
  public void lock(String lockName, long timeout) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void unlock(String lockName) {
    // TODO Auto-generated method stub
    
  }
  
  private Object indexKey(String edgeKind, Object key) {
    return  composite("@" + edgeKind, key);
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
