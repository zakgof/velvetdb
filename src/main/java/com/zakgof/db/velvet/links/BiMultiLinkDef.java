package com.zakgof.db.velvet.links;

import static com.zakgof.db.velvet.VelvetUtil.kindOf;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public class BiMultiLinkDef<A, B> extends ABiLinkDef<A, B, MultiLinkDef<A, B>, BiParentLinkDef<B, A>> implements IMultiLinkDef<A, B> {
  public BiMultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(new MultiLinkDef<>(aClazz, bClazz, edgeKind));
  }

  public static <A, B> BiMultiLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz) {
    return of(aClazz, bClazz, kindOf(aClazz) + "-" + kindOf(bClazz), kindOf(bClazz) + "-" + kindOf(aClazz));
  }

  public static <A, B> BiMultiLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {
    BiMultiLinkDef<A, B> link = new BiMultiLinkDef<A, B>(aClazz, bClazz, edgeKind);
    BiParentLinkDef<B, A> backLink = new BiParentLinkDef<B, A>(bClazz, aClazz, backLinkKind);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  @Override
  public List<B> links(IVelvet velvet, A node) {
    return oneWay.links(velvet, node);
  }

  @Override
  public List<Object> linkKeys(IVelvet velvet, Object key) {
    return oneWay.linkKeys(velvet, key);
  }
  
  // TODO : this is code duplication
  public void addChild(IVelvet velvet, A a, B b) {    
    velvet.put(b);
    connect(velvet, a, b);
  }

}
