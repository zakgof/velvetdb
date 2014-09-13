package com.zakgof.db.graph.datadef;

public interface IGetter<A, B> {

  public String getKind();

  public Class<A> getHostClass();

  public Class<B> getChildClass();
}
