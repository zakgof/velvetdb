package com.zakgof.db.velvet.links;

import com.zakgof.db.velvet.IVelvet;

public class BiSingleLinkDef<A, B> extends ABiLinkDef<A, B, SingleLinkDef<A, B>, BiSingleLinkDef<B, A>> implements ISingleLinkDef<A, B> {

  private BiSingleLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(new SingleLinkDef<>(aClazz, bClazz, edgeKind));
  }

  public static <A, B> BiSingleLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {
    BiSingleLinkDef<A, B> link = new BiSingleLinkDef<A, B>(aClazz, bClazz, edgeKind);
    BiSingleLinkDef<B, A> backLink = new BiSingleLinkDef<B, A>(bClazz, aClazz, backLinkKind);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  @Override
  public B single(IVelvet velvet, A node) {
    return oneWay.single(velvet, node);
  }

  @Override
  public Object singleKey(IVelvet velvet, Object key) {
    return oneWay.singleKey(velvet, key);
  }

}
