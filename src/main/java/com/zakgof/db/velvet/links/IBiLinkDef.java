package com.zakgof.db.velvet.links;

public interface IBiLinkDef<A, B> extends ILinkDef<A, B> {

  public IBiLinkDef<B, A> back();

}
