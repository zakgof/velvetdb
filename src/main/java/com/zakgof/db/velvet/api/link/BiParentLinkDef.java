package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class BiParentLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, IBiMultiLinkDef<CK, CV, HK, HV>>implements IBiParentLinkDef<HK, HV, CK, CV> {

  BiParentLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
  }
  
  BiParentLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity, edgeKind));
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
  
  @Override
  public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
    CK oldChildKey = singleKey(velvet, akey);
    if (oldChildKey != null)
      backLink.disconnectKeys(velvet, oldChildKey, akey);
    super.connectKeys(velvet, akey, bkey);
  }

}
