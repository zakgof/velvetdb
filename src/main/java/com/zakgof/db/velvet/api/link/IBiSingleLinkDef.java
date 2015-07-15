package com.zakgof.db.velvet.api.link;

public interface IBiSingleLinkDef<HK, HV, CK, CV> extends ISingleLinkDef<HK, HV, CK, CV>, IBiLinkDef<HK, HV, CK, CV, IBiSingleLinkDef<CK, CV, HK, HV>> {
}
