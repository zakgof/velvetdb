package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;

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
    connectKeys(velvet, VelvetUtil.keyOf(a), VelvetUtil.keyOf(b));
  }

  @Override
  public void disconnect(IVelvet velvet, A a, B b) {
    disconnectKeys(velvet, VelvetUtil.keyOf(a), VelvetUtil.keyOf(b));
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
