package com.zakgof.db.velvet.links;

import java.util.List;

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
    List<?> keys = velvet.raw().linkKeys(VelvetUtil.keyClassOf(getChildClass()), key, edgeKind);
    if (keys.isEmpty())
      return null;
    if (keys.size() != 1)
      throw new RuntimeException("Multiple values under singleKey");
    return keys.get(0);
  }
  

  public void disconnect(IVelvet velvet, A a) {
    disconnect(velvet, a, single(velvet, a));
  }

  public void disconnectAllByKey(IVelvet velvet, Object akey) {
    Object bkey = singleKey(velvet, akey);
    if (bkey != null)
      disconnectKeys(velvet, akey, bkey);
  }

  public void addChild(IVelvet velvet, A a, B b) {
    checkExisting(velvet, VelvetUtil.keyOf(a));    
    velvet.put(b);
    connect(velvet, a, b);
  }
  
  @Override
  public void connect(IVelvet velvet, A a, B b) {
    checkExisting(velvet, VelvetUtil.keyOf(a));  
    super.connect(velvet, a, b);
  }
  
  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    checkExisting(velvet, akey);
    super.connectKeys(velvet, akey, bkey);
  }

  private void checkExisting(IVelvet velvet, Object akey) {
    Object existingBKey = singleKey(velvet, akey);
    if (existingBKey != null)
      throw new RuntimeException("Attempt to connect multiple objects to " + toString() + " " + akey + " -> " + existingBKey); // TODO : own exception type
  }
  
}
