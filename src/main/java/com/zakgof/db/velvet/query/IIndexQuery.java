package com.zakgof.db.velvet.query;

public interface IIndexQuery<K extends Comparable<K>> {
  
  IQueryAnchor<K> getLowAnchor();

  IQueryAnchor<K> getHighAnchor();

  int getLimit();

  int getOffset();

  boolean isAscending();
  
}