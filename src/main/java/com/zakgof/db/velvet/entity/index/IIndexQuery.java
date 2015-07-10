package com.zakgof.db.velvet.entity.index;


public interface IIndexQuery<K> {
   IQueryAnchor getLowAnchor();
   IQueryAnchor getHighAnchor();
   int getLimit();
   int getOffset();
   boolean isAscending();
}