package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class BiParentLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, BiMultiLinkDef<CK, CV, HK, HV>>implements ISingleLinkDef<HK, HV, CK, CV> {

  BiParentLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
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
    return "BiParentLinkDef " + super.toString();
  }

}
