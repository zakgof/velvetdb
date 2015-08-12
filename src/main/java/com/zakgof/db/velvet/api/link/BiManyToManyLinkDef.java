package com.zakgof.db.velvet.api.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

class BiManyToManyLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, MultiLinkDef<HK, HV, CK, CV>, IBiManyToManyLinkDef<CK, CV, HK, HV>>implements IBiManyToManyLinkDef<HK, HV, CK, CV> {

  private BiManyToManyLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new MultiLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
  }

  private BiManyToManyLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    super(new MultiLinkDef<HK, HV, CK, CV>(hostEntity, childEntity, edgeKind));
  }

  static <HK, HV, CK, CV> BiManyToManyLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    BiManyToManyLinkDef<HK, HV, CK, CV> link = new BiManyToManyLinkDef<HK, HV, CK, CV>(hostEntity, childEntity);
    BiManyToManyLinkDef<CK, CV, HK, HV> backLink = new BiManyToManyLinkDef<CK, CV, HK, HV>(childEntity, hostEntity);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  static <HK, HV, CK, CV> BiManyToManyLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
    BiManyToManyLinkDef<HK, HV, CK, CV> link = new BiManyToManyLinkDef<HK, HV, CK, CV>(hostEntity, childEntity, edgeKind);
    BiManyToManyLinkDef<CK, CV, HK, HV> backLink = new BiManyToManyLinkDef<CK, CV, HK, HV>(childEntity, hostEntity, backEdgeKind);
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
