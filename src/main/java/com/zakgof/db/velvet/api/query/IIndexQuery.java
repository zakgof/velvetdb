package com.zakgof.db.velvet.api.query;


public interface IIndexQuery<K> {
   IQueryAnchor getLowAnchor();
   IQueryAnchor getHighAnchor();
   int getLimit();
   int getOffset();
   boolean isAscending();
}