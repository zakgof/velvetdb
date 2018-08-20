package com.zakgof.db.velvet.impl.link;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiParentLinkDef;
import com.zakgof.db.velvet.link.IBiSecMultiLinkDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.query.ISecQuery;
import com.zakgof.db.velvet.query.ISingleReturnSecQuery;

public class BiSecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends ABiLinkDef<HK, HV, CK, CV, SecIndexMultiLinkDef<HK, HV, CK, CV, M>, IBiParentLinkDef<CK, CV, HK, HV>>
        implements IBiSecMultiLinkDef<HK, HV, CK, CV, M> {

    private BiSecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
        super(new SecIndexMultiLinkDef<>(hostEntity, childEntity, mclazz, metric));
    }

    public static <HK, HV, CK, CV, M extends Comparable<? super M>> BiSecIndexMultiLinkDef<HK, HV, CK, CV, M> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
        BiSecIndexMultiLinkDef<HK, HV, CK, CV, M> link = new BiSecIndexMultiLinkDef<>(hostEntity, childEntity, mclazz, metric);
        BiParentLinkDef<CK, CV, HK, HV> backLink = new BiParentLinkDef<>(childEntity, hostEntity);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }

    @Override
    public List<CV> get(IVelvet velvet, HV node) {
        return oneWay.get(velvet, node);
    }

    @Override
    public List<CK> keys(IVelvet velvet, HK key) {
        return oneWay.keys(velvet, key);
    }

    @Override
    public Map<HK, List<CV>> batchGet(IVelvet velvet, List<HV> nodes) {
        return oneWay.batchGet(velvet, nodes);
    }

    @Override
    public Map<HK, List<CK>> batchKeys(IVelvet velvet, List<HK> keys) {
        return oneWay.batchKeys(velvet, keys);
    }

    @Override
    public IMultiGetter<HK, HV, CK, CV> indexed(ISecQuery<CK, M> indexQuery) {
        return oneWay.indexed(indexQuery);
    }

    @Override
    public ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnSecQuery<CK, M> indexQuery) {
        return oneWay.indexedSingle(indexQuery);
    }

    @Override
    public Function<CV, M> getMetric() {
        return oneWay.getMetric();
    }
}
