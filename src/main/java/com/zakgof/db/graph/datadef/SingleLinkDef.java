package com.zakgof.db.graph.datadef;

import java.util.List;

import com.zakgof.db.graph.IPersister;
import com.zakgof.db.graph.PersisterUtil;

public class SingleLinkDef<A, B> extends ALinkDef<A, B> implements ISingleGetter<A, B> {

  public SingleLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(aClazz, bClazz, edgeKind);
  }

  public static <A, B> SingleLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz) {
    return of(aClazz, bClazz, PersisterUtil.kindOf(aClazz) + "-" + PersisterUtil.kindOf(bClazz));
  }

  public static <A, B> SingleLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    return new SingleLinkDef<A, B>(aClazz, bClazz, edgeKind);
  }

  public B single(IPersister persister, A node) {
    return persister.singleLink(bClazz, node, edgeKind);
  }

  public Object singleKey(IPersister persister, Object key) {

    List<Object> keys = persister.session().linkKeys(Object.class, key, edgeKind);
    if (keys.isEmpty())
      return null;
    if (keys.size() != 1)
      throw new RuntimeException("Multiple values under singleKey");
    return keys.get(0);
  }

  public void disconnect(IPersister persister, A a) {
    disconnect(persister, a, single(persister, a));
  }

  public void disconnectAllByKey(IPersister persister, Object akey) {
    Object bkey = singleKey(persister, akey);
    if (bkey != null)
      disconnectKeys(persister, akey, bkey);
  }

  public void addChild(IPersister persister, A a, B b) {
    // delete old child
    Object akey = PersisterUtil.keyOf(a);
    Object oldChildKey = singleKey(persister, akey);
    if (oldChildKey != null) {
      disconnectKeys(persister, akey, oldChildKey);
      persister.session().delete(PersisterUtil.kindOf(bClazz), oldChildKey);
    }
    persister.put(b);
    connect(persister, a, b);
  }
  
  @Override
  public String toString() {
    return "single " + edgeKind + " " + getHostClass() + "->" + getChildClass();
  }

}
