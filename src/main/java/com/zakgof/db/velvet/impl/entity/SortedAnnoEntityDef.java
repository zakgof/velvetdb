package com.zakgof.db.velvet.impl.entity;

import com.zakgof.db.velvet.entity.ISortableEntityDef;

public class SortedAnnoEntityDef<K extends Comparable<K>, V> extends SortedEntityDef<K, V> implements ISortableEntityDef<K, V> {

  public SortedAnnoEntityDef(Class<V> valueClass, AnnoKeyProvider<K, V> annoKeyProvider) {
    super(annoKeyProvider.getKeyClass(), valueClass, AnnoEntityDef.kindOf(valueClass), annoKeyProvider);
  }
}
