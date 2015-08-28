package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class PriIndexMultiLinkDef<HK, HV, CK extends Comparable<CK>, CV> extends AIndexMultiLinkDef<HK, HV, CK, CV, CK> implements IIndexedMultiGetter<HK, HV, CK, CV, CK> {

  public PriIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(hostEntity, childEntity);
  }

  protected IKeyIndexLink<CK, CK> index(IVelvet velvet, HK akey) {
    return velvet.<CK, CV> primaryKeyIndex(akey, getKind());
  }

}
