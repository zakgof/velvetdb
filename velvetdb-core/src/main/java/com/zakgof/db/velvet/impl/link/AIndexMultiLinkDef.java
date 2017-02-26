package com.zakgof.db.velvet.impl.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.ISortedMultiLink;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.query.IRangeQuery;
import com.zakgof.db.velvet.query.ISingleReturnRangeQuery;

abstract class AIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends MultiLinkDef<HK, HV, CK, CV> implements ISortedMultiLink<HK, HV, CK, CV, M> {

    public AIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    abstract protected IKeyIndexLink<CK, M> index(IVelvet velvet, HK akey);

    @Override
    public IMultiGetter<HK, HV, CK, CV> indexed(IRangeQuery<CK, M> indexQuery) {

        return new IMultiGetter<HK, HV, CK, CV>() {

            @Override
            public List<CV> multi(IVelvet velvet, HV node) {
                return getChildEntity().get(velvet, multiKeys(velvet, getHostEntity().keyOf(node)));
            }

            @Override
            public List<CK> multiKeys(IVelvet velvet, HK akey) {
                return index(velvet, akey).keys(getChildEntity().getKeyClass(), indexQuery);
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
            public CV single(IVelvet velvet, HV node) {
                CK singleKey = singleKey(velvet, getHostEntity().keyOf(node));
                return singleKey == null ? null : getChildEntity().get(velvet, singleKey);
            }

            @Override
            public CK singleKey(IVelvet velvet, HK akey) {
                return index(velvet, akey).key(getChildEntity().getKeyClass(), indexQuery);
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
