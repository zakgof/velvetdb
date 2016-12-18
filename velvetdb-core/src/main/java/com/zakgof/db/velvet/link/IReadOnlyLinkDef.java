package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.IVelvet;

public interface IReadOnlyLinkDef<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {

    public String getKind();

    public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey);

    public boolean isConnected(IVelvet velvet, HV a, CV b);
}
