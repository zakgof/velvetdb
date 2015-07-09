package com.zakgof.db.velvet.entity;

import com.zakgof.db.velvet.IVelvet;

class ABiLinkDef<HK, HV, CK, CV, OneWayLinkType extends ILinkDef<HK, HV, CK, CV>, BackLinkType extends ABiLinkDef<CK, CV, HK, HV, ?, ?>> extends ALinkDef<HK, HV, CK, CV> implements IBiLinkDef<HK, HV, CK, CV> {

  protected OneWayLinkType oneWay;
  protected BackLinkType backLink;

  protected ABiLinkDef(OneWayLinkType oneWay) {
    super(oneWay.getHostEntity(), oneWay.getChildEntity(), oneWay.getKind());
    this.oneWay = oneWay;
  }

  protected void setBackLink(BackLinkType backLink) {
    this.backLink = backLink;
  }

  @Override
  public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
    oneWay.connectKeys(velvet, akey, bkey);
    backLink.oneWay.connectKeys(velvet, bkey, akey);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, HK akey, CK bkey) {
    oneWay.disconnectKeys(velvet, akey, bkey);
    backLink.oneWay.disconnectKeys(velvet, bkey, akey);
  }

  @Override
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey) {
    return oneWay.isConnectedKeys(velvet, akey, bkey);  
  }

  @Override
  public String getKind() {
    return oneWay.getKind();
  }

  @Override
  public BackLinkType back() {
    return backLink;
  } 
}
