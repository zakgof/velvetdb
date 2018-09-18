package com.zakgof.db.velvet.impl.link;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISecIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.ISecMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.link.Links;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

public class SecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends MultiLinkDef<HK, HV, CK, CV> implements ISecMultiLinkDef<HK, HV, CK, CV, M> {

    private final Function<CV, M> metric;
    private Class<M> mclazz;

    public SecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric, String edgeKind) {
        super(hostEntity, childEntity, edgeKind);
        this.metric = metric;
        this.mclazz = mclazz;
    }

    public SecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
        super(hostEntity, childEntity);
        this.metric = metric;
        this.mclazz = mclazz;
    }

    @Override
    protected ISecIndexLink<HK, CK, M> index(IVelvet velvet) {
        return velvet.<HK, CK, CV, M> secondaryKeyIndex(getHostEntity().getKeyClass(), getKind(), metric, mclazz, getChildEntity().getKeyClass(), ((EntityDef<CK, CV>) getChildEntity()).store(velvet));
    }

    @Override
    public Function<CV, M> getMetric() {
        return metric;
    }

    @Override
    public IMultiGetter<HK, HV, CK, CV> indexed(ISecQuery<CK, M> indexQuery) {
        return new IMultiGetter<HK, HV, CK, CV>() {

            @Override
            public IEntityDef<HK, HV> getHostEntity() {
                return SecIndexMultiLinkDef.this.getHostEntity();
            }

            @Override
            public IEntityDef<CK, CV> getChildEntity() {
                return SecIndexMultiLinkDef.this.getChildEntity();
            }

            @Override
            public List<CV> get(IVelvet velvet, HV node) {
                return new ArrayList<>(getChildEntity().batchGetMap(velvet, keys(velvet, getHostEntity().keyOf(node))).values());
            }

            @Override
            public List<CK> keys(IVelvet velvet, HK key) {
                return index(velvet).keys(key, indexQuery);
            }
        };
    }

    @Override
    public ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnSecQuery<CK, M> indexQuery) {
        return Links.toSingleGetter(indexed((ISecQuery<CK, M>) indexQuery));
    }
}
