package com.zakgof.db.velvet.api.entity.impl;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISortedStore;
import com.zakgof.db.velvet.api.entity.ISortableEntityDef;
import com.zakgof.db.velvet.api.query.IIndexQuery;

public class SortedEntityDef<K extends Comparable<K>, V> extends EntityDef<K, V> implements ISortableEntityDef<K, V> {
  
  SortedEntityDef(Class<K> keyClass, Class<V> valueClass, String kind, Function<V, K> keyProvider) {
    super(keyClass, valueClass, kind, keyProvider);
  }

  @Override
  ISortedStore<K, V> store(IVelvet velvet) {
    return velvet.sortedStore(getKind(), getKeyClass(), getValueClass());
  }

  @Override
  public List<K> keys(IVelvet velvet, IIndexQuery<K> query) {
    return store(velvet).keys(query);
  }
}
