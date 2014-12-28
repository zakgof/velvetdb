package com.zakgof.db.velvet.query;


public interface IIndexQuery<K> {
   IQueryAnchor getLowAnchor();
   IQueryAnchor getHighAnchor();
   int getLimit();
   int getOffset();
   boolean isAscending();
}