package com.zakgof.db.velvet.impl.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ILink;
import com.zakgof.db.velvet.IVelvet.LinkType;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.ISingleLinkDef;

public class SingleLinkDef<HK, HV, CK, CV> extends AVelvetLinkDef<HK, HV, CK, CV> implements ISingleLinkDef<HK, HV, CK, CV> {

    public SingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(hostEntity, childEntity, edgeKind);
    }

    public SingleLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    @Override
    public CV single(IVelvet velvet, HV node) {
        CK bkey = singleKey(velvet, getHostEntity().keyOf(node));
        return bkey == null ? null : getChildEntity().get(velvet, bkey);
    }

    @Override
    public CK singleKey(IVelvet velvet, HK key) {
        List<CK> linkKeys = (List<CK>) index(velvet, key).keys(getChildEntity().getKeyClass());
        return linkKeys.isEmpty() ? null : linkKeys.get(0);
    }

    ILink<CK> index(IVelvet velvet, HK akey) {
        return velvet.simpleIndex(akey, getKind(), LinkType.Single);
    }

    @Override
    public String toString() {
        return "single " + super.toString();
    }

}
