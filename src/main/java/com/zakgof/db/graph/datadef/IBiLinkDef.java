package com.zakgof.db.graph.datadef;


public interface IBiLinkDef<A, B> extends ILinkDef<A, B> {

  public ISingleGetter<B, A> back();

}
