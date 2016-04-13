package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.IVelvet;

public interface IReadOnlyLinkDef<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {
  
  public String getKind();
  
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey);
  
  public default boolean isConnected(IVelvet velvet, HV a, CV b) {
    return isConnectedKeys(velvet, getHostEntity().keyOf(a), getChildEntity().keyOf(b));
  }
}
