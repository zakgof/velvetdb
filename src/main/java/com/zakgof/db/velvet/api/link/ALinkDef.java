package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.entity.impl.Entities;

abstract class ALinkDef<HK, HV, CK, CV> implements ILinkDef<HK, HV, CK, CV> {

  private String edgeKind;
  private IEntityDef<HK, HV> hostEntity;
  private IEntityDef<CK, CV> childEntity;

  ALinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
    this.hostEntity = hostEntity;
    this.childEntity = childEntity;
    this.edgeKind = edgeKind;
  }

  ALinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    this(hostEntity, childEntity, hostEntity.getKind() + "-" + childEntity.getKind());
  }

  ALinkDef(Class<HV> hostClass, Class<CV> childClass) {
    this(Entities.anno(hostClass), Entities.anno(childClass));
  }

  @Override
  public String getKind() {
    return edgeKind;
  }

  @Override
  public IEntityDef<HK, HV> getHostEntity() {
    return hostEntity;
  }

  @Override
  public IEntityDef<CK, CV> getChildEntity() {
    return childEntity;
  }

  @Override
  public void connect(IVelvet velvet, HV a, CV b) {
    connectKeys(velvet, getHostEntity().keyOf(a), getChildEntity().keyOf(b));
  }

  @Override
  public String toString() {
    return edgeKind + " : " + hostEntity.getKind() + " -> " + childEntity.getKind();
  }
}
