package com.zakgof.db.velvet.impl.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ISingleLink;
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
    public CV get(IVelvet velvet, HV node) {
        CK bkey = key(velvet, getHostEntity().keyOf(node));
        return bkey == null ? null : getChildEntity().get(velvet, bkey);
    }

    @Override
    public CK key(IVelvet velvet, HK key) {
        List<CK> linkKeys = index(velvet).keys(key);
        return linkKeys.isEmpty() ? null : linkKeys.get(0);
    }

    @Override
    ISingleLink<HK, CK> index(IVelvet velvet) {
        return velvet.singleLink(getHostEntity().getKeyClass(), getChildEntity().getKeyClass(), getKind());
    }

    @Override
    public String toString() {
        return "single " + super.toString();
    }

}
