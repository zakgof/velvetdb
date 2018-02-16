package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.IVelvet;

public interface ISingleGetter<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {

    public CV get(IVelvet velvet, HV node);

    public CK key(IVelvet velvet, HK key);

}
