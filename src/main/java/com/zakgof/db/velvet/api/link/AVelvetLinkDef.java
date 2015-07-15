package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ILink;
import com.zakgof.db.velvet.api.entity.IEntityDef;

abstract class AVelvetLinkDef<HK, HV, CK, CV> extends ALinkDef<HK, HV, CK, CV> {

  AVelvetLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
   super(hostEntity, childEntity, edgeKind);
  }

  AVelvetLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(hostEntity, childEntity);
  }

  AVelvetLinkDef(Class<HV> hostClass, Class<CV> childClass) {
    super(hostClass, childClass);
  }

  abstract ILink<CK> index(IVelvet velvet, HK hostKey);

  @Override
  public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
    index(velvet, akey).connect(bkey);
  }

  @Override
  public void disconnectKeys(IVelvet velvet, HK akey, CK bkey) {
    index(velvet, akey).disconnect(bkey);
  }
  
  @Override
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey) {
    return index(velvet, akey).isConnected(bkey);
  }

}
