package com.zakgof.db.velvet.impl.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.link.IBiLinkDef;
import com.zakgof.db.velvet.link.ILinkDef;

class ABiLinkDef<HK, HV, CK, CV, OneWayLinkType extends ILinkDef<HK, HV, CK, CV>, BackLinkType extends IBiLinkDef<CK, CV, HK, HV, ?>> extends ALinkDef<HK, HV, CK, CV> implements IBiLinkDef<HK, HV, CK, CV, BackLinkType> {

  protected OneWayLinkType oneWay;
  protected BackLinkType backLink;

  protected ABiLinkDef(OneWayLinkType oneWay) {
    super(oneWay.getHostEntity(), oneWay.getChildEntity(), oneWay.getKind());
    this.oneWay = oneWay;
  }

  protected void setBackLink(BackLinkType backLink) {
    this.backLink = backLink;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
    oneWay.connectKeys(velvet, akey, bkey);    
    ((ABiLinkDef< CK, CV, HK, HV, ?, ?>)backLink).oneWay.connectKeys(velvet, bkey, akey);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void disconnectKeys(IVelvet velvet, HK akey, CK bkey) {
    oneWay.disconnectKeys(velvet, akey, bkey);
    ((ABiLinkDef< CK, CV, HK, HV, ?, ?>)backLink).oneWay.disconnectKeys(velvet, bkey, akey);
  }

  @Override
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey) {
    return oneWay.isConnectedKeys(velvet, akey, bkey);
  }

  @Override
  public BackLinkType back() {
    return backLink;
  }
}
