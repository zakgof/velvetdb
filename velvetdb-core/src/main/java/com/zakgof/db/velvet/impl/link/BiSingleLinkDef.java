package com.zakgof.db.velvet.impl.link;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiSingleLinkDef;

public class BiSingleLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, IBiSingleLinkDef<CK, CV, HK, HV>> implements IBiSingleLinkDef<HK, HV, CK, CV> {

    private BiSingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(new SingleLinkDef<>(hostEntity, childEntity));
    }

    private BiSingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(new SingleLinkDef<>(hostEntity, childEntity, edgeKind));
    }

    public static <HK, HV, CK, CV> BiSingleLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        BiSingleLinkDef<HK, HV, CK, CV> link = new BiSingleLinkDef<>(hostEntity, childEntity);
        BiSingleLinkDef<CK, CV, HK, HV> backLink = new BiSingleLinkDef<>(childEntity, hostEntity);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }

    public static <HK, HV, CK, CV> BiSingleLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        BiSingleLinkDef<HK, HV, CK, CV> link = new BiSingleLinkDef<>(hostEntity, childEntity, edgeKind);
        BiSingleLinkDef<CK, CV, HK, HV> backLink = new BiSingleLinkDef<>(childEntity, hostEntity, backEdgeKind);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }

    @Override
    public CV get(IVelvet velvet, HV node) {
        return oneWay.get(velvet, node);
    }

    @Override
    public CK key(IVelvet velvet, HK key) {
        return oneWay.key(velvet, key);
    }

    @Override
    public Map<HK, CV> batchGet(IVelvet velvet, List<HV> nodes) {
        return oneWay.batchGet(velvet, nodes);
    }

    @Override
    public Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
        return oneWay.batchKeys(velvet, keys);
    }

    @Override
    public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
        CK oldChildKey = key(velvet, akey);
        if (oldChildKey != null)
            backLink.disconnectKeys(velvet, oldChildKey, akey);
        super.connectKeys(velvet, akey, bkey);
    }

    @Override
    public String toString() {
        return "BiSingleLinkDef " + super.toString();
    }

}
