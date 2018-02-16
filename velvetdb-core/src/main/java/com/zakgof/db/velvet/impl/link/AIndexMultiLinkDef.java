package com.zakgof.db.velvet.impl.link;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.link.ISortedMultiLink;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

abstract class AIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends MultiLinkDef<HK, HV, CK, CV> implements ISortedMultiLink<HK, HV, CK, CV, M> {

    public AIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    @Override
    abstract protected IKeyIndexLink<HK, CK, M> index(IVelvet velvet);

    @Override
    public IMultiGetter<HK, HV, CK, CV> indexed(IRangeQuery<CK, M> indexQuery) {

        return new IMultiGetter<HK, HV, CK, CV>() {

            @Override
            public List<CV> get(IVelvet velvet, HV node) {
                return new ArrayList<>(getChildEntity().batchGet(velvet, keys(velvet, getHostEntity().keyOf(node))).values());
            }

            @Override
            public List<CK> keys(IVelvet velvet, HK akey) {
                return index(velvet).keys(akey, indexQuery);
            }

            @Override
            public IEntityDef<HK, HV> getHostEntity() {
                return AIndexMultiLinkDef.this.getHostEntity();
            }

            @Override
            public IEntityDef<CK, CV> getChildEntity() {
                return AIndexMultiLinkDef.this.getChildEntity();
            }
        };
    }

    @Override
    public ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnRangeQuery<CK, M> indexQuery) {
        return new ISingleGetter<HK, HV, CK, CV>() {
            @Override
            public CV get(IVelvet velvet, HV node) {
                CK singleKey = key(velvet, getHostEntity().keyOf(node));
                return singleKey == null ? null : getChildEntity().get(velvet, singleKey);
            }

            @Override
            public CK key(IVelvet velvet, HK akey) {
                return index(velvet).key(akey, indexQuery);
            }

            @Override
            public IEntityDef<HK, HV> getHostEntity() {
                return AIndexMultiLinkDef.this.getHostEntity();
            }

            @Override
            public IEntityDef<CK, CV> getChildEntity() {
                return AIndexMultiLinkDef.this.getChildEntity();
            }
        };
    }
}
