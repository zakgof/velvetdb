package com.zakgof.db.velvet.entity;

public interface IBiLinkDef<HK, HV, CK, CV> extends ILinkDef<HK, HV, CK, CV> {

  public IBiLinkDef<CK, CV, HK, HV> back();

}
