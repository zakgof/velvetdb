package com.zakgof.db.velvet.api.entity.impl;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IStore;

public class SortedAnnoEntityDef<K extends Comparable<K>, V> extends AnnoEntityDef<K, V> {
  
  SortedAnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider) {
    super(valueClass, annoKeyProvider);
  }

  @Override
  IStore<K, V> store(IVelvet velvet) {
    return velvet.sortedStore(getKind(), getKeyClass(), getValueClass());
  }
}
