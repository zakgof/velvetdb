package com.zakgof.db.velvet.links;

import java.util.List;

import com.zakgof.db.velvet.IRawVelvet.LinkType;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;

public class SingleLinkDef<A, B> extends ALinkDef<A, B> implements ISingleLinkDef<A, B> {

  public SingleLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(aClazz, bClazz, edgeKind);
  }

  public static <A, B> SingleLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz) {
    return of(aClazz, bClazz, VelvetUtil.kindOf(aClazz) + "-" + VelvetUtil.kindOf(bClazz));
  }

  public static <A, B> SingleLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    return new SingleLinkDef<A, B>(aClazz, bClazz, edgeKind);
  }

  @Override
  public B single(IVelvet velvet, A node) {
    Object bkey = singleKey(velvet, VelvetUtil.keyOf(node));
    return bkey == null ? null : velvet.get(bClazz, bkey);
  }

  private Object singleKey(IVelvet velvet, Object key) {    
    List<?> linkKeys = velvet.raw().index(key, edgeKind, LinkType.Single).linkKeys(VelvetUtil.keyClassOf(getChildClass()));
    return linkKeys.isEmpty() ? null : linkKeys.get(0);    
  }

  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    velvet.raw().index(akey, edgeKind, LinkType.Single).connect(bkey);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    velvet.raw().index(akey, edgeKind, LinkType.Single).disconnect(bkey);
  }
 
  
}
