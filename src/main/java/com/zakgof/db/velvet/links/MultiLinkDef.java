package com.zakgof.db.velvet.links;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.VelvetUtil;

public class MultiLinkDef<A, B> extends ALinkDef<A, B> implements IMultiLinkDef<A, B> {

  public MultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(aClazz, bClazz, edgeKind);
  }
  
  public static <A, B> MultiLinkDef<A, B> of (Class<A> aClazz, Class<B> bClazz) {
    return of(aClazz, bClazz, VelvetUtil.kindOf(aClazz) + "-" + VelvetUtil.kindOf(bClazz));
  }

  public static <A, B> MultiLinkDef<A, B> of (Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    return new MultiLinkDef<A, B>(aClazz, bClazz, edgeKind);
  }
  
  public List<B> links(IVelvet velvet, A node) {
    return velvet.links(bClazz, node, edgeKind);
  }
  
  public List<Object> linkKeys(IVelvet velvet, Object key) {
    return velvet.raw().linkKeys(Object.class, key, edgeKind);
  }

  public void disconnectAll(IVelvet velvet, A node) {
    // TODO : lock
    List<B> links = links(velvet, node);
    for (B b : links)
      disconnect(velvet, node, b);
  }
  
  public void disconnectAllByKey(IVelvet velvet, Object akey) {
    List<Object> linkKeys = linkKeys(velvet, akey);
    for (Object bkey : linkKeys)
      disconnectKeys(velvet, akey, bkey);
  }
  
  public void addChild(IVelvet velvet, A a, B b) {
    velvet.put(b);
    connect(velvet, a, b);    
  }
  
  @Override
  public String toString() {
    return "multi " + edgeKind + " " + getHostClass() + "->" + getChildClass();
  }
  
}
