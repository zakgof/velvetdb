package com.zakgof.db.velvet.entity;

import com.zakgof.db.velvet.IVelvet;

public class BiSingleLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, BiSingleLinkDef<CK, CV, HK, HV>>implements ISingleLinkDef<HK, HV, CK, CV> {

  private BiSingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
  }

  public static <HK, HV, CK, CV> BiSingleLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
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
