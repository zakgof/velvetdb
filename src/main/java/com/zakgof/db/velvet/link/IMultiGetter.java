package com.zakgof.db.velvet.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;

public interface IMultiGetter<HK, HV, CK, CV> extends IRelation <HK, HV, CK, CV> {

  public List<CV> multi(IVelvet velvet, HV node);

  public List<CK> multiKeys(IVelvet velvet, HK key);

}
