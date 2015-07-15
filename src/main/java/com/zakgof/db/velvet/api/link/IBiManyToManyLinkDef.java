package com.zakgof.db.velvet.api.link;

public interface IBiManyToManyLinkDef<HK, HV, CK, CV> extends IMultiLinkDef<HK, HV, CK, CV>, IBiLinkDef<HK, HV, CK, CV, IBiManyToManyLinkDef<CK, CV, HK, HV>> {
}
