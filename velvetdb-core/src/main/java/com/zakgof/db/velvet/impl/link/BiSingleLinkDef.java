package com.zakgof.db.velvet.impl.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiSingleLinkDef;

public class BiSingleLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, IBiSingleLinkDef<CK, CV, HK, HV>> implements IBiSingleLinkDef<HK, HV, CK, CV> {

    private BiSingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity));
    }

    private BiSingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(new SingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity, edgeKind));
    }

    public static <HK, HV, CK, CV> BiSingleLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        BiSingleLinkDef<HK, HV, CK, CV> link = new BiSingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity);
        BiSingleLinkDef<CK, CV, HK, HV> backLink = new BiSingleLinkDef<CK, CV, HK, HV>(childEntity, hostEntity);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }

    public static <HK, HV, CK, CV> BiSingleLinkDef<HK, HV, CK, CV> create(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind, String backEdgeKind) {
        BiSingleLinkDef<HK, HV, CK, CV> link = new BiSingleLinkDef<HK, HV, CK, CV>(hostEntity, childEntity, edgeKind);
        BiSingleLinkDef<CK, CV, HK, HV> backLink = new BiSingleLinkDef<CK, CV, HK, HV>(childEntity, hostEntity, backEdgeKind);
        link.setBackLink(backLink);
        backLink.setBackLink(link);
        return link;
    }

    @Override
    public CV single(IVelvet velvet, HV node) {
        return oneWay.single(velvet, node);
    }

    @Override
    public CK singleKey(IVelvet velvet, HK key) {
        return oneWay.singleKey(velvet, key);
    }

    @Override
    public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
        CK oldChildKey = singleKey(velvet, akey);
        if (oldChildKey != null)
            backLink.disconnectKeys(velvet, oldChildKey, akey);
        super.connectKeys(velvet, akey, bkey);
    }

    @Override
    public String toString() {
        return "BiSingleLinkDef " + super.toString();
    }

}
