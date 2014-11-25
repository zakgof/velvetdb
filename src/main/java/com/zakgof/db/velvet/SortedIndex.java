package com.zakgof.db.velvet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import com.zakgof.db.kvs.ITransactionalKvs;
import com.zakgof.db.velvet.kvs.GenericKvsVelvet3;
import com.zakgof.db.velvet.links.index.IndexQuery;

interface ILink<K> {
  void connect(Object key2);
  void disconnect(Object key2);
  List<K> getAll();
}

interface ISortedLink<K, C> extends ILink<K> {
  List<K> get(IndexQuery<K, C> query);
}

public class SortedIndex<K, C> implements ISortedLink<K, C> {
  
  private final ITransactionalKvs kvs;
  
  private final Object key1;
  private final Class<K> clazz;

  private final IndexQuery<B, C> query;

  SortedIndex(Object key1, Class<K> clazz, IndexQuery<B, C> query) {
    this.key1 = key1;
    this.clazz = clazz;
    this.query =  query;
  }
  
  public void connect(K key2) {
    // TODO : perf : use arrays
    K[] index = kvs.get(GenericKvsVelvet3.<K>getArrayClass(clazz), link(key1));
    List<K> list = index == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(index)); // TODO : perf
    int insertIndex = search(list, key2);
    list.add(insertIndex, key2);    
    kvs.put(link(key1), list);    
  }
  
  private int search(List<K> list, K key2) {
    int i1 = 0;
    int i2 = list.get();
  }

  public void disconnect(K key2) {
    K[] index = kvs.get(GenericKvsVelvet3.<K>getArrayClass(clazz), link(key1));
    List<K> list = index == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(index)); // TODO : perf
    int pos = Arrays.binarySearch(index, key2);
    if (pos < 0)
      throw new NoSuchElementException();
    if (list.size() == 1)
      kvs.delete(link(key1));
    else {
      list.remove(pos);
      kvs.put(link(key1), list);
    }
  }

  @Override
  public List<K> getAll() {
    K[] index = kvs.get(GenericKvsVelvet3.<K>getArrayClass(clazz), link(key1));
    List<K> list = index == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(index)); // TODO : perf
    return list;
  }

  @Override
  public List<K> get(IndexQuery<K, C> query) {
    query.l1
    return null;
  }
  

}
