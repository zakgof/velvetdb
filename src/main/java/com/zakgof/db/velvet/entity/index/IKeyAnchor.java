package com.zakgof.db.velvet.entity.index;


public interface IKeyAnchor<K> extends IQueryAnchor {
  K getKey();
}