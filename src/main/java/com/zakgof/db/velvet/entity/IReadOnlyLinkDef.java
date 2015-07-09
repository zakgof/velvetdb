package com.zakgof.db.velvet.entity;

import com.zakgof.db.velvet.IVelvet;

public interface IReadOnlyLinkDef<HK, HV, CK, CV> {
  
  public String getKind();

  public IEntityDef<HK, HV> getHostEntity();
  
  public IEntityDef<CK, CV> getChildEntity();
  
  public boolean isConnected(IVelvet velvet, HV a, CV b);
  
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey);
}
