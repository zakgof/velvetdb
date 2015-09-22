package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.query.IIndexQuery;

public interface ISortableEntityDef<K extends Comparable<K>, V> extends IEntityDef<K, V> {

  public List<K> keys(IVelvet velvet, IIndexQuery<K> query);

  public default List<V> get(IVelvet velvet, IIndexQuery<K> query) {
    return get(velvet, keys(velvet, query));
  }

}
