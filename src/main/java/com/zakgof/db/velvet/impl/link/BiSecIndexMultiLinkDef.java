package com.zakgof.db.velvet.impl.link;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiMultiLinkDef;
import com.zakgof.db.velvet.link.IBiParentLinkDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.ISecIndexMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.query.IIndexQuery;
import com.zakgof.db.velvet.query.ISingleReturnIndexQuery;

public class BiSecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends ABiLinkDef<HK, HV, CK, CV, SecIndexMultiLinkDef<HK, HV, CK, CV, M>, IBiParentLinkDef<CK, CV, HK, HV>> implements IBiMultiLinkDef<HK, HV, CK, CV>, ISecIndexMultiLinkDef<HK, HV, CK, CV, M> {

  private BiSecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
    super(new SecIndexMultiLinkDef<HK, HV, CK, CV, M>(hostEntity, childEntity, mclazz, metric));
  }
  
  public static <HK, HV, CK, CV, M extends Comparable<? super M>> BiSecIndexMultiLinkDef<HK, HV, CK, CV, M> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
    BiSecIndexMultiLinkDef<HK, HV, CK, CV, M> link = new BiSecIndexMultiLinkDef<>(hostEntity, childEntity, mclazz, metric);
    BiParentLinkDef<CK, CV, HK, HV> backLink = new BiParentLinkDef<CK, CV, HK, HV>(childEntity, hostEntity);
    link.setBackLink(backLink);
    backLink.setBackLink(link);
    return link;
  }

  @Override
  public List<CV> multi(IVelvet velvet, HV node) {
    return oneWay.multi(velvet, node);
  }

  @Override
  public List<CK> multiKeys(IVelvet velvet, HK key) {
    return oneWay.multiKeys(velvet, key);
  }

	@Override
	public IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<CK, M> indexQuery) {
		return oneWay.indexed(indexQuery);
	}

	@Override
	public ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnIndexQuery<CK, M> indexQuery) {
		return oneWay.indexedSingle(indexQuery);
	}
	
	public Function<CV, M> getMetric() {
		return oneWay.getMetric();
	}
}
