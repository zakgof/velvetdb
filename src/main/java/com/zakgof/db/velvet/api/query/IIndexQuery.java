package com.zakgof.db.velvet.api.query;


public interface IIndexQuery {
   IQueryAnchor getLowAnchor();
   IQueryAnchor getHighAnchor();
   int getLimit();
   int getOffset();
   boolean isAscending();
}