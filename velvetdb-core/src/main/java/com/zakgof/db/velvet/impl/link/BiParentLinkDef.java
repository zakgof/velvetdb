package com.zakgof.db.velvet.impl.link;

import java.util.List;
import java.util.Map;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IBiMultiLinkDef;
import com.zakgof.db.velvet.link.IBiParentLinkDef;

public class BiParentLinkDef<HK, HV, CK, CV> extends ABiLinkDef<HK, HV, CK, CV, SingleLinkDef<HK, HV, CK, CV>, IBiMultiLinkDef<CK, CV, HK, HV>> implements IBiParentLinkDef<HK, HV, CK, CV> {

    BiParentLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(new SingleLinkDef<>(hostEntity, childEntity));
    }

    BiParentLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(new SingleLinkDef<>(hostEntity, childEntity, edgeKind));
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
    public String toString() {
        return "BiParentLinkDef " + super.toString();
    }

    @Override
    public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
        CK oldChildKey = key(velvet, akey);
        if (oldChildKey != null)
            backLink.disconnectKeys(velvet, oldChildKey, akey);
        super.connectKeys(velvet, akey, bkey);
    }

}
