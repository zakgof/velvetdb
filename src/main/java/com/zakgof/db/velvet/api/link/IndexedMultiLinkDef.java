package com.zakgof.db.velvet.api.link;

import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IRawVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.query.IIndexQuery;

public class IndexedMultiLinkDef<HK, HV, CK, CV, C extends Comparable<C>> extends MultiLinkDef<HK, HV, CK, CV>implements IIndexedMultiGetter<HK, HV, CK, CV, C> {

  private final Function<CV, C> metric;

  public IndexedMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Function<CV, C> metric) {
    super(hostEntity, childEntity);
    this.metric = metric;

  }

  protected IKeyIndexLink<CK> index(IVelvet velvet, HK akey) {
    return velvet.raw().<CK, CV, C> index(akey, getKind(), getChildEntity().getValueClass(), getChildEntity().getKind(), metric);
  }

  @Override
  public IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<CK> indexQuery) {
    
    return new IMultiGetter<HK, HV, CK, CV>() {

      @Override
      public List<CV> multi(IVelvet velvet, HV node) {
        return getChildEntity().getAll(velvet, multiKeys(velvet, getHostEntity().keyOf(node)));
      }

      @Override
      public List<CK> multiKeys(IVelvet velvet, HK akey) {
        return index(velvet, akey).linkKeys(getChildEntity().getKeyClass(), indexQuery);
      }
    };
  }

}
