package com.zakgof.db.velvet.old;

import com.zakgof.db.velvet.IVelvet;

class ABiLinkDef<A, B, OneWayLinkType extends ILinkDef<A, B>, BackLinkType extends ABiLinkDef<B, A, ?, ?>> implements IBiLinkDef<A, B> {

  protected OneWayLinkType oneWay;
  private BackLinkType backLink;

  protected ABiLinkDef(OneWayLinkType oneWay) {
    this.oneWay = oneWay;
  }

  protected void setBackLink(BackLinkType backLink) {
    this.backLink = backLink;
  }

  @Override
  public void connect(IVelvet velvet, A a, B b) {
    oneWay.connect(velvet, a, b);
    backLink.oneWay.connect(velvet, b, a);
  }

  @Override
  public void disconnect(IVelvet velvet, A a, B b) {
    oneWay.disconnect(velvet, a, b);
    backLink.oneWay.disconnect(velvet, b, a);
  }

  @Override
  public void connectKeys(IVelvet velvet, Object akey, Object bkey) {
    oneWay.connectKeys(velvet, akey, bkey);
    backLink.oneWay.connectKeys(velvet, bkey, akey);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, Object akey, Object bkey) {
    oneWay.disconnectKeys(velvet, akey, bkey);
    backLink.oneWay.disconnectKeys(velvet, bkey, akey);
  }
  
  @Override
  public boolean isConnected(IVelvet velvet, A a, B b) {
    return oneWay.isConnected(velvet, a, b);    
  }
  
  @Override
  public boolean isConnectedKeys(IVelvet velvet, Object akey, Object bkey) {
    return oneWay.isConnectedKeys(velvet, akey, bkey);  
  }

  @Override
  public String getKind() {
    return oneWay.getKind();
  }

  @Override
  public Class<A> getHostClass() {
    return oneWay.getHostClass();
  }

  @Override
  public Class<B> getChildClass() {
    return oneWay.getChildClass();
  }

  @Override
  public BackLinkType back() {
    return backLink;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + " : " + getHostClass().getSimpleName() + " -> " + getChildClass().getSimpleName() + " [" + getKind() + "]";
  }
}
