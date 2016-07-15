package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.IVelvet;

public interface ISingleGetter<HK, HV, CK, CV> extends IRelation <HK, HV, CK, CV> {

  public CV single(IVelvet velvet, HV node);

  public CK singleKey(IVelvet velvet, HK key);

}
