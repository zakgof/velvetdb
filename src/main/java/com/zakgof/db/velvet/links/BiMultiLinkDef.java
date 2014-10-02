package com.zakgof.db.velvet.links;

import static com.zakgof.db.velvet.VelvetUtil.kindOf;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public class BiMultiLinkDef<A, B> extends ABiLinkDef<A, B, MultiLinkDef<A, B>, BiParentLinkDef<B, A>> implements IMultiLinkDef<A, B> {
  
  protected BiMultiLinkDef(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {
    this(new MultiLinkDef<>(aClazz, bClazz, edgeKind), backLinkKind);        
  }

  protected BiMultiLinkDef(MultiLinkDef<A, B> oneWay, String backLinkKind) {
    super(oneWay);
    BiParentLinkDef<B, A> backLink = new BiParentLinkDef<B, A>(oneWay.getChildClass(), oneWay.getHostClass(), backLinkKind);
    setBackLink(backLink);
    backLink.setBackLink(this);
  }

  public static <A, B> BiMultiLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz) {
    return of(aClazz, bClazz, kindOf(aClazz) + "-" + kindOf(bClazz), kindOf(bClazz) + "-" + kindOf(aClazz));
  }

  public static <A, B> BiMultiLinkDef<A, B> of(Class<A> aClazz, Class<B> bClazz, String edgeKind, String backLinkKind) {    
    return new BiMultiLinkDef<A, B>(aClazz, bClazz, edgeKind, backLinkKind);
  }
  
  public static <A, B> BiMultiLinkDef<A, B> of(MultiLinkDef<A, B> oneWay, String backLinkKind) {    
    return new BiMultiLinkDef<A, B>(oneWay, backLinkKind);
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
