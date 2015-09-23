package com.zakgof.db.velvet.impl.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISortedStore;
import com.zakgof.db.velvet.entity.ISortableEntityDef;
import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.ISingleReturnIndexQuery;

// TODO: avoid duplication SortedEntityDef
public class SortedAnnoEntityDef<K extends Comparable<K>, V> extends AnnoEntityDef<K, V> implements ISortableEntityDef<K, V> {
  
  public SortedAnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider) {
    super(valueClass, annoKeyProvider);
  }

  @Override
  ISortedStore<K, V> store(IVelvet velvet) {
    return velvet.sortedStore(getKind(), getKeyClass(), getValueClass());
  }

  @Override
  public List<K> keys(IVelvet velvet, IIndexQuery<K> query) {
    return store(velvet).keys(query);
  }
  
  @Override
  public K key(IVelvet velvet, ISingleReturnIndexQuery<K> query) {
    List<K> keys = keys(velvet, query);
    if (keys.size() > 1)
      throw new RuntimeException("ISingleReturnIndexQuery returned multiple entries");
    return keys.isEmpty() ? null : keys.get(0);
  }
}
