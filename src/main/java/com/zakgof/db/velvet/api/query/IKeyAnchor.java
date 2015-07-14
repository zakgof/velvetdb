package com.zakgof.db.velvet.api.query;


public interface IKeyAnchor<K> extends IQueryAnchor {
  K getKey();
}