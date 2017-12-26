package com.zakgof.db.velvet.impl.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.ISortedMultiGetter;

public class PriIndexMultiLinkDef<HK, HV, CK extends Comparable<CK>, CV> extends AIndexMultiLinkDef<HK, HV, CK, CV, CK> implements ISortedMultiGetter<HK, HV, CK, CV, CK> {

    public PriIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    @Override
    protected IKeyIndexLink<HK, CK, CK> index(IVelvet velvet, HK akey) {
        return velvet.<HK, CK, CV> primaryKeyIndex(akey, getHostEntity().getKeyClass(), getChildEntity().getKeyClass(), getKind());
    }

}
