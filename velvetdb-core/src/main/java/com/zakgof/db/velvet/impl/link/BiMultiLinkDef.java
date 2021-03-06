package com.zakgof.db.velvet.impl.link;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiMultiLinkDef;
import com.zakgof.db.velvet.link.IBiParentLinkDef;

public class BiMultiLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, MultiLinkDef<HK, HV, CK, CV>, IBiParentLinkDef<CK, CV, HK, HV>> implements IBiMultiLinkDef<HK, HV, CK, CV> {

    private BiMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(new MultiLinkDef<>(hostEntity, childEntity));
    }

    private BiMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(new MultiLinkDef<>(hostEntity, childEntity, edgeKind));
    }

    public static <HK, HV, CK, CV> BiMultiLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        BiMultiLinkDef<HK, HV, CK, CV> link = new BiMultiLinkDef<>(hostEntity, childEntity);
        BiParentLinkDef<CK, CV, HK, HV> backLink = new BiParentLinkDef<>(childEntity, hostEntity);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }

    public static <HK, HV, CK, CV> BiMultiLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        BiMultiLinkDef<HK, HV, CK, CV> link = new BiMultiLinkDef<>(hostEntity, childEntity, edgeKind);
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
}
