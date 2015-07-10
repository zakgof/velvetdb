package com.zakgof.db.velvet.entity.index;


public interface ISecondaryIndexAnchor<M> extends IQueryAnchor {
  M getValue();
}