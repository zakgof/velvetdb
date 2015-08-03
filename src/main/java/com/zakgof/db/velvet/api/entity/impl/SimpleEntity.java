package com.zakgof.db.velvet.api.entity.impl;

import com.zakgof.db.velvet.api.entity.IEntityDef;

public class SimpleEntity<K> implements IEntityDef<K, K> {

  private final Class<K> clazz;
  private final String kind;

  SimpleEntity(Class<K> clazz, String kind) {
    this.clazz = clazz;
    this.kind = kind;
  }

  @Override
  public Class<K> getKeyClass() {
    return clazz;
  }

  @Override
  public Class<K> getValueClass() {
    return clazz;
  }

  @Override
  public K keyOf(K value) {
    return value;
  }

  @Override
  public String getKind() {
    return kind;
  }

}
