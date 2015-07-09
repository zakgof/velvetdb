package com.zakgof.db.velvet.entity;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public class BiManyToManyLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, MultiLinkDef<HK, HV, CK, CV>, BiManyToManyLinkDef<CK, CV, HK, HV>>implements IMultiLinkDef<HK, HV, CK, CV> {

  private BiManyToManyLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(new MultiLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
  }
  
  private BiManyToManyLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String backEdgeKind) {
    super(new MultiLinkDef<HK, HV, CK, CV>(hostEntity, childEntity, backEdgeKind));
  }

  public static <HK, HV, CK, CV> BiManyToManyLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    BiManyToManyLinkDef<HK, HV, CK, CV> link = new BiManyToManyLinkDef<HK, HV, CK, CV>(hostEntity, childEntity);
    BiManyToManyLinkDef<CK, CV, HK, HV> backLink = new BiManyToManyLinkDef<CK, CV, HK, HV>(childEntity, hostEntity);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  public static <HK, HV, CK, CV> BiManyToManyLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
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