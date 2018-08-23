package com.zakgof.db.velvet.impl.link;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IPriIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.IPriMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

public class PriIndexMultiLinkDef<HK, HV, CK extends Comparable<? super CK>, CV> extends MultiLinkDef<HK, HV, CK, CV> implements IPriMultiLinkDef<HK, HV, CK, CV> {

    public PriIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    @Override
    protected IPriIndexLink<HK, CK> index(IVelvet velvet) {
        return velvet.<HK, CK> primaryKeyIndex(getHostEntity().getKeyClass(), getChildEntity().getKeyClass(), getKind());
    }

    @Override
    public IMultiGetter<HK, HV, CK, CV> indexed(IKeyQuery<CK> indexQuery) {

        return new IMultiGetter<HK, HV, CK, CV>() {

            @Override
            public IEntityDef<HK, HV> getHostEntity() {
                return PriIndexMultiLinkDef.this.getHostEntity();
            }

            @Override
            public IEntityDef<CK, CV> getChildEntity() {
                return PriIndexMultiLinkDef.this.getChildEntity();
            }

            @Override
            public List<CV> get(IVelvet velvet, HV node) {
                return new ArrayList<>(getChildEntity().batchGetMap(velvet, keys(velvet, getHostEntity().keyOf(node))).values());
            }

            @Override
            public List<CK> keys(IVelvet velvet, HK akey) {
                return index(velvet).keys(akey, indexQuery);
            }

        };
    }

    @Override
    public ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnKeyQuery<CK> indexQuery) {
        return Links.toSingleGetter(indexed((IKeyQuery<CK>) indexQuery));
    }

}
