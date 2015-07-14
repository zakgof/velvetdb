package com.zakgof.db.velvet.api.link;

public interface IBiLinkDef<HK, HV, CK, CV> extends ILinkDef<HK, HV, CK, CV> {

  public IBiLinkDef<CK, CV, HK, HV> back();

}
