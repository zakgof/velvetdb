package com.zakgof.db.velvet.link;

public interface IBiLinkDef<HK, HV, CK, CV, BackLinkType extends IBiLinkDef<CK, CV, HK, HV, ?>> extends ILinkDef<HK, HV, CK, CV> {

    public BackLinkType back();

}
