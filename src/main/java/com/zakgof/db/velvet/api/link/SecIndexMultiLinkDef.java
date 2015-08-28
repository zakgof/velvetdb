package com.zakgof.db.velvet.api.link;

import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.api.entity.IEntityDef;

public class SecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<M>> extends AIndexMultiLinkDef<HK, HV, CK, CV, M>implements IIndexedMultiGetter<HK, HV, CK, CV, M> {

  private final Function<CV, M> metric;
  private Class<M> mclazz;

  public SecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
    super(hostEntity, childEntity);
    this.metric = metric;
    this.mclazz = mclazz;
  }
  
  protected IKeyIndexLink<CK, M> index(IVelvet velvet, HK akey) {
    return velvet.<CK, CV, M> secondaryKeyIndex(akey, getKind(), getChildEntity().getValueClass(), getChildEntity().getKind(), metric, mclazz);
  }
}
