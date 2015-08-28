package com.zakgof.db.velvet.api.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.query.IIndexQuery;

abstract class AIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<M>> extends MultiLinkDef<HK, HV, CK, CV> implements IIndexedMultiLink<HK, HV, CK, CV, M> {

  public AIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(hostEntity, childEntity);
  }

  abstract protected IKeyIndexLink<CK, M> index(IVelvet velvet, HK akey);

  @Override
  public IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<M> indexQuery) {

    return new IMultiGetter<HK, HV, CK, CV>() {

      @Override
      public List<CV> multi(IVelvet velvet, HV node) {
        return getChildEntity().get(velvet, multiKeys(velvet, getHostEntity().keyOf(node)));
      }

      @Override
      public List<CK> multiKeys(IVelvet velvet, HK akey) {
        return index(velvet, akey).keys(getChildEntity().getKeyClass(), indexQuery);
      }
    };
  }

}
