package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public interface IReadOnlyLinkDef<HK, HV, CK, CV> {
  
  public String getKind();

  public IEntityDef<HK, HV> getHostEntity();
  
  public IEntityDef<CK, CV> getChildEntity();
  
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey);
  
  public default boolean isConnected(IVelvet velvet, HV a, CV b) {
    return isConnectedKeys(velvet, getHostEntity().keyOf(a), getChildEntity().keyOf(b));
  }
}
