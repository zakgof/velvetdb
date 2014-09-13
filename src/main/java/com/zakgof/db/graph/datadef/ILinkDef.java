package com.zakgof.db.graph.datadef;

import com.zakgof.db.graph.IPersister;

public interface ILinkDef<A, B> {

  public void connect(IPersister persister, A a, B b);

  public void connectKeys(IPersister persister, Object akey, Object bkey);

  public void disconnect(IPersister persister, A a, B b);

  public void disconnectKeys(IPersister persister, Object akey, Object bkey);

  public void disconnectAllByKey(IPersister persister, Object akey);
  
  public String getKind();

  public Class<A> getHostClass();

  public Class<B> getChildClass();

}