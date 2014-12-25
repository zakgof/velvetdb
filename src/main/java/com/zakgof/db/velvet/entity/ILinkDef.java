package com.zakgof.db.velvet.entity;

import com.zakgof.db.velvet.IVelvet;

public interface ILinkDef<HK, HV, CK, CV> {
  
  public String getKind();

  public IEntityDef<HK, HV> getHostEntity();
  
  public IEntityDef<HK, HV> getChildEntity();
 
  public void connect(IVelvet velvet, HV a, CV b);

  public void connectKeys(IVelvet velvet, HK akey, CK bkey);

  public void disconnect(IVelvet velvet, HV a, CV b);

  public void disconnectKeys(IVelvet velvet, HK akey, CK bkey);
  
  public boolean isConnected(IVelvet velvet, HV a, CV b);
  
  public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey);
}
