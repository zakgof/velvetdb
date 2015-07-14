package com.zakgof.db.velvet.api.query;


public interface ISecondaryIndexAnchor<M> extends IQueryAnchor {
  M getValue();
}