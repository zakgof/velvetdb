package com.zakgof.db.velvet.link;

public interface IBiParentLinkDef<HK, HV, CK, CV> extends ISingleLinkDef<HK, HV, CK, CV>, IBiLinkDef<HK, HV, CK, CV, IBiMultiLinkDef<CK, CV, HK, HV>> {
}
