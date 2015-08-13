package com.zakgof.db.velvet.api.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.api.entity.IEntityDef;
import com.zakgof.db.velvet.api.query.IIndexQuery;

public class PriIndexMultiLinkDef<HK, HV, CK extends Comparable<CK>, CV> extends MultiLinkDef<HK, HV, CK, CV>implements IIndexedMultiGetter<HK, HV, CK, CV, CK> {

  public PriIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
    super(hostEntity, childEntity);
  }
  
  protected IKeyIndexLink<CK> index(IVelvet velvet, HK akey) {
    return velvet.<CK, CV> index(akey, getKind());
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
