package com.zakgof.db.velvet.api.link;

import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class SecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<M>> extends AIndexMultiLinkDef<HK, HV, CK, CV, M>implements IIndexedMultiGetter<HK, HV, CK, CV, M> {

  private final Function<CV, M> metric;

  public SecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Function<CV, M> metric) {
    super(hostEntity, childEntity);
    this.metric = metric;
  }
  
  protected IKeyIndexLink<CK> index(IVelvet velvet, HK akey) {
    return velvet.<CK, CV, M> index(akey, getKind(), getChildEntity().getValueClass(), getChildEntity().getKind(), metric);
  }
}
