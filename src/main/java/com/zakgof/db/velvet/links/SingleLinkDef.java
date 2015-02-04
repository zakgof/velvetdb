package com.zakgof.db.velvet.links;

import java.util.List;

import com.zakgof.db.velvet.IRawVelvet.ILink;
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

  @Override
  public Object singleKey(IVelvet velvet, Object key) {    
    @SuppressWarnings("unchecked")
    List<?> linkKeys = index(velvet, key).linkKeys((Class<Object>) VelvetUtil.keyClassOf(getChildClass()));
    return linkKeys.isEmpty() ? null : linkKeys.get(0);    
  }

  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).connect(bkey);
  }

  private ILink<Object> index(IVelvet velvet, Object akey) {
    return velvet.raw().index(akey, edgeKind, LinkType.Single);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    index(velvet, akey).disconnect(bkey);
  }
  
  @Override
  public boolean isConnectedKeys(IVelvet velvet, Object akey, Object bkey) {
    return index(velvet, akey).isConnected(bkey);
  }
  
 
  
}
