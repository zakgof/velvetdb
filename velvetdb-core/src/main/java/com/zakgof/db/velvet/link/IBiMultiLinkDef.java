package com.zakgof.db.velvet.link;

public interface IBiMultiLinkDef<HK, HV, CK, CV> extends IMultiLinkDef<HK, HV, CK, CV>, IBiLinkDef<HK, HV, CK, CV, IBiParentLinkDef<CK, CV, HK, HV>> {
}
