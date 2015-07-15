package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

class BiSingleLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, IBiSingleLinkDef<CK, CV, HK, HV>> implements IBiSingleLinkDef<HK, HV, CK, CV> {

  BiSingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
  }

  static <HK, HV, CK, CV> BiSingleLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    BiSingleLinkDef<HK, HV, CK, CV> link = new BiSingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity);
    BiSingleLinkDef<CK, CV, HK, HV> backLink = new BiSingleLinkDef<CK, CV, HK, HV>(childEntity, hostEntity);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  @Override
  public CV single(IVelvet velvet, HV node) {
    return oneWay.single(velvet, node);
  }

  @Override
  public CK singleKey(IVelvet velvet, HK key) {
    return oneWay.singleKey(velvet, key);
  }

  @Override
  public String toString() {
    return "BiSingleLinkDef " + super.toString();
  }

}
