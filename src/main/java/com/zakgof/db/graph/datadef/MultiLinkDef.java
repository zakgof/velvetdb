package com.zakgof.db.graph.datadef;

import java.util.List;

import com.zakgof.db.graph.IPersister;

public class MultiLinkDef<A, B> extends ALinkDef<A, B> implements IMultiGetter<A, B> {

  public MultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(aClazz, bClazz, edgeKind);
  }

  public static <A, B> MultiLinkDef<A, B> of (Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    return new MultiLinkDef<A, B>(aClazz, bClazz, edgeKind);
  }
  
  public List<B> links(IPersister persister, A node) {
    return persister.links(bClazz, node, edgeKind);
  }
  
  public List<Object> linkKeys(IPersister persister, Object key) {
    return persister.session().linkKeys(Object.class, key, edgeKind);
  }

  public void disconnectAll(IPersister persister, A node) {
    // TODO : lock
    List<B> links = links(persister, node);
    for (B b : links)
      disconnect(persister, node, b);
  }
  
  public void disconnectAllByKey(IPersister persister, Object akey) {
    List<Object> linkKeys = linkKeys(persister, akey);
    for (Object bkey : linkKeys)
      disconnectKeys(persister, akey, bkey);
  }
  
  public void addChild(IPersister persister, A a, B b) {
    persister.put(b);
    connect(persister, a, b);    
  }
  
  @Override
  public String toString() {
    return "multi " + edgeKind + " " + getHostClass() + "->" + getChildClass();
  }
  
}
