package com.zakgof.db.graph.datadef;

import com.zakgof.db.graph.IPersister;

public class BiMultiLinkDef<A, B> extends MultiLinkDef<A, B> implements IBiLinkDef<A, B> {

  private final SingleLinkDef<B, A> backLink;

  public BiMultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {
    super(aClazz, bClazz, edgeKind);
    this.backLink = new SingleLinkDef<>(bClazz, aClazz, backLinkKind);    
  }

  public static <A, B> BiMultiLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {
    return new BiMultiLinkDef<A, B>(aClazz, bClazz, edgeKind, backLinkKind);
  }

  @Override
  public void connect(IPersister persister, A a, B b) {
    super.connect(persister, a, b);
    backLink.connect(persister, b, a);
  }

  public void connectKeys(IPersister persister, Object akey, Object bkey) {
    super.connectKeys(persister, akey, bkey);
    backLink.connectKeys(persister, bkey, akey);
  }

  @Override
  public void disconnect(IPersister persister, A a, B b) {
    super.disconnect(persister, a, b);
    backLink.disconnect(persister, b, a);
  }

  @Override
  public void disconnectKeys(IPersister persister, Object akey, Object bkey) {
    super.disconnectKeys(persister, akey, bkey);
    backLink.disconnectKeys(persister, bkey, akey);
  }

  @Override
  public ISingleGetter<B, A> back() {
    return backLink;
  }

}
