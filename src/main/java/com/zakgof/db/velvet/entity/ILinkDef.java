package com.zakgof.db.velvet.entity;

import com.zakgof.db.velvet.IVelvet;

public interface ILinkDef<HK, HV, CK, CV> extends IReadOnlyLinkDef<HK, HV, CK, CV> {

  public void connect(IVelvet velvet, HV a, CV b);

  public void connectKeys(IVelvet velvet, HK akey, CK bkey);

  public void disconnect(IVelvet velvet, HV a, CV b);

  public void disconnectKeys(IVelvet velvet, HK akey, CK bkey);

}
