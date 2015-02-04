package com.zakgof.db.velvet.links;

import static com.zakgof.db.velvet.VelvetUtil.kindOf;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public class BiManyToManyLinkDef<A, B> extends ABiLinkDef<A, B, MultiLinkDef<A, B>, BiManyToManyLinkDef<B, A>> implements IMultiLinkDef<A, B> {

  public BiManyToManyLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind) {
    super(new MultiLinkDef<>(aClazz, bClazz, edgeKind));
  }

  public static <A, B> BiManyToManyLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz) {
    return of(aClazz, bClazz, kindOf(aClazz) + "-" + kindOf(bClazz), kindOf(bClazz) + "-" + kindOf(aClazz));
  }

  public static <A, B> BiManyToManyLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {
    BiManyToManyLinkDef<A, B> link = new BiManyToManyLinkDef<A, B>(aClazz, bClazz, edgeKind);
    BiManyToManyLinkDef<B, A> backLink = new BiManyToManyLinkDef<B, A>(bClazz, aClazz, backLinkKind);
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

}
