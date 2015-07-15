package com.zakgof.db.velvet.old;

public interface IBiLinkDef<A, B> extends ILinkDef<A, B> {

  public IBiLinkDef<B, A> back();

}
