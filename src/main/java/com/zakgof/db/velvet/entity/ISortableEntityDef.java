package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.ISingleReturnIndexQuery;

public interface ISortableEntityDef<K extends Comparable<? super K>, V> extends IEntityDef<K, V> {

  public List<K> keys(IVelvet velvet, IIndexQuery<K, K> query);
  
  public K key(IVelvet velvet, ISingleReturnIndexQuery<K, K> query);

  public default List<V> get(IVelvet velvet, IIndexQuery<K, K> query) {
    return get(velvet, keys(velvet, query));
  }
  
  public default V get(IVelvet velvet, ISingleReturnIndexQuery<K, K> query) {
    K key = key(velvet, query);
    return key == null ? null : get(velvet, key);
  }

}
