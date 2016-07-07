package com.zakgof.db.velvet.impl.link;

import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.link.ISecSortedMultiLinkDef;

public class SecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends AIndexMultiLinkDef<HK, HV, CK, CV, M> implements ISecSortedMultiLinkDef<HK, HV, CK, CV, M> {

  private final Function<CV, M> metric;
  private Class<M> mclazz;

  public SecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
    super(hostEntity, childEntity);
    this.metric = metric;
    this.mclazz = mclazz;
  }
  
  protected IKeyIndexLink<CK, M> index(IVelvet velvet, HK akey) {
    return velvet.<CK, CV, M> secondaryKeyIndex(akey, getKind(), metric, mclazz, getChildEntity().getKeyClass(), ((EntityDef<CK, CV>)getChildEntity()).store(velvet));
  }
  
  public Function<CV, M> getMetric() {
	return metric;
  }
}
