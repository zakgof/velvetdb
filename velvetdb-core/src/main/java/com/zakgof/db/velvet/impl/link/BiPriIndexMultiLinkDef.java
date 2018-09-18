package com.zakgof.db.velvet.impl.link;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiParentLinkDef;
import com.zakgof.db.velvet.link.IBiPriMultiLinkDef;
import com.zakgof.db.velvet.link.IMultiGetter;
import com.zakgof.db.velvet.link.ISingleGetter;
import com.zakgof.db.velvet.query.IKeyQuery;
import com.zakgof.db.velvet.query.ISingleReturnKeyQuery;

public class BiPriIndexMultiLinkDef<HK, HV, CK extends Comparable<? super CK>, CV> extends ABiLinkDef<HK, HV, CK, CV, PriIndexMultiLinkDef<HK, HV, CK, CV>, IBiParentLinkDef<CK, CV, HK, HV>>
        implements IBiPriMultiLinkDef<HK, HV, CK, CV> {

    private BiPriIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String egdeKind) {
        super(new PriIndexMultiLinkDef<>(hostEntity, childEntity, egdeKind));
    }

    private BiPriIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(new PriIndexMultiLinkDef<>(hostEntity, childEntity));
    }

    public static <HK, HV, CK extends Comparable<? super CK>, CV> BiPriIndexMultiLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        BiPriIndexMultiLinkDef<HK, HV, CK, CV> link = new BiPriIndexMultiLinkDef<>(hostEntity, childEntity);
        BiParentLinkDef<CK, CV, HK, HV> backLink = new BiParentLinkDef<>(childEntity, hostEntity);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }


    public static <HK, HV, CK extends Comparable<? super CK>, CV> IBiPriMultiLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        BiPriIndexMultiLinkDef<HK, HV, CK, CV> link = new BiPriIndexMultiLinkDef<>(hostEntity, childEntity, edgeKind);
        BiParentLinkDef<CK, CV, HK, HV> backLink = new BiParentLinkDef<>(childEntity, hostEntity, backEdgeKind);
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
    public IMultiGetter<HK, HV, CK, CV> indexed(IKeyQuery<CK> indexQuery) {
        return oneWay.indexed(indexQuery);
    }

    @Override
    public ISingleGetter<HK, HV, CK, CV> indexedSingle(ISingleReturnKeyQuery<CK> indexQuery) {
        return oneWay.indexedSingle(indexQuery);
    }
}
