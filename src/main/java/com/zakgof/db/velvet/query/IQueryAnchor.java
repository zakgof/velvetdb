package com.zakgof.db.velvet.query;

public interface IQueryAnchor<K extends Comparable<K>> {
  
  boolean isIncluding();

  K getKey();
  
}