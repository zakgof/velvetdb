package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;

public interface ILinkDef<A, B> {

  public String getKind();

  public Class<A> getHostClass();

  public Class<B> getChildClass();

  public void connect(IVelvet velvet, A a, B b);

  public void connectKeys(IVelvet velvet, Object akey, Object bkey);

  public void disconnect(IVelvet velvet, A a, B b);

  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey);

}