package com.zakgof.db.velvet.query;


public interface IKeyAnchor<K> extends IQueryAnchor {
  K getKey();
}