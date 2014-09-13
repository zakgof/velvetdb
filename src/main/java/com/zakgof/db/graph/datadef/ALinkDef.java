package com.zakgof.db.graph.datadef;

import com.zakgof.db.graph.IPersister;


abstract class ALinkDef<A, B> implements ILinkDef<A, B> {
  private final Class<A> aClazz;
  protected final Class<B> bClazz;
  protected final String edgeKind;
  
  public ALinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    this.aClazz = aClazz;
    this.bClazz = bClazz;
    this.edgeKind = edgeKind;
  }

  @Override
  public void connect(IPersister persister, A a, B b) {
    persister.connect(a, b, edgeKind);
  }
  
  @Override
  public void connectKeys(IPersister persister,  Object akey, Object bkey) {
    persister.session().connect(akey, bkey, edgeKind);
  }
  
  @Override
  public void disconnect(IPersister persister, A a, B b) {
    persister.disconnect(a, b, edgeKind);
  }
  
  @Override
  public void disconnectKeys(IPersister persister, Object akey, Object bkey) {
    persister.session().disconnect(akey, bkey, edgeKind);
  }
  
  @Override
  public String getKind() {
    return edgeKind;
  }
  
  @Override
  public Class<A> getHostClass() {
    return aClazz;
  }
  
  @Override
  public Class<B> getChildClass() {
    return bClazz;
  }

}
