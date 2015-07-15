package com.zakgof.db.velvet.api.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

class BiMultiLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, MultiLinkDef<HK, HV, CK, CV>, IBiParentLinkDef<CK, CV, HK, HV>> implements IBiMultiLinkDef<HK, HV, CK, CV> {

  private BiMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new MultiLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
  }

  static <HK, HV, CK, CV> BiMultiLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    BiMultiLinkDef<HK, HV, CK, CV> link = new BiMultiLinkDef<HK, HV, CK, CV>(hostEntity, childEntity);
    BiParentLinkDef<CK, CV, HK, HV> backLink = new BiParentLinkDef<CK, CV, HK, HV>(childEntity, hostEntity);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  @Override
  public List<CV> multi(IVelvet velvet, HV node) {
    return oneWay.multi(velvet, node);
  }

  @Override
  public List<CK> multiKeys(IVelvet velvet, HK key) {
    return oneWay.multiKeys(velvet, key);
  }
}
