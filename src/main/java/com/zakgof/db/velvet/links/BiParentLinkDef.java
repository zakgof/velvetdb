package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;

public class BiParentLinkDef<A, B> extends ABiLinkDef<A, B, SingleLinkDef<A, B>, BiMultiLinkDef<B, A>> implements ISingleLinkDef<A, B> {

  BiParentLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(new SingleLinkDef<>(aClazz, bClazz, edgeKind));
  }

  @Override
  public B single(IVelvet velvet, A node) {
    return oneWay.single(velvet, node);
  }

  @Override
  public Object singleKey(IVelvet velvet, Object key) {
    return oneWay.singleKey(velvet, key);
  }
  
  @Override
  public String toString() {
    return "BiParentLinkDef " + getHostClass().getSimpleName() + " -> " + getChildClass().getSimpleName();
  }

}
