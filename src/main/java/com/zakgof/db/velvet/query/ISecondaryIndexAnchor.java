package com.zakgof.db.velvet.query;


public interface ISecondaryIndexAnchor<M> extends IQueryAnchor {
  M getValue();
}