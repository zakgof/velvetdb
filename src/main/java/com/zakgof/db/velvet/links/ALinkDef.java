package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;


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
  public void connect(IVelvet velvet, A a, B b) {
    velvet.connect(a, b, edgeKind);
  }
  
  @Override
  public void connectKeys(IVelvet velvet,  Object akey, Object bkey) {
    velvet.raw().connect(akey, bkey, edgeKind);
  }
  
  @Override
  public void disconnect(IVelvet velvet, A a, B b) {
    velvet.disconnect(a, b, edgeKind);
  }
  
  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    velvet.raw().disconnect(akey, bkey, edgeKind);
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

  @Override
  public String toString() {
    return getClass().getSimpleName() + " : " + getHostClass().getSimpleName() + "->" + getChildClass().getSimpleName() + " [" + edgeKind + "]";
  }

}
