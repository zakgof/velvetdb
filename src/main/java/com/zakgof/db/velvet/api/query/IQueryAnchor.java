package com.zakgof.db.velvet.api.query;

public interface IQueryAnchor<K extends Comparable<K>> {
  
  boolean isIncluding();

  K getKey();
  
}