package com.zakgof.db.velvet.impl.link;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ILink;
import com.zakgof.db.velvet.VelvetException;
import com.zakgof.db.velvet.entity.IEntityDef;

abstract class AVelvetLinkDef<HK, HV, CK, CV> extends ALinkDef<HK, HV, CK, CV> {

    AVelvetLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(hostEntity, childEntity, edgeKind);
    }

    AVelvetLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    AVelvetLinkDef(Class<HV> hostClass, Class<CV> childClass) {
        super(hostClass, childClass);
    }

    abstract ILink<HK, CK> index(IVelvet velvet);

    @Override
    public void connectKeys(IVelvet velvet, HK akey, CK bkey) {
        if (akey == null || bkey == null) {
            throw new VelvetException("Attempt to connect a null key");
        }
        index(velvet).put(akey, bkey);
    }

    @Override
    public void disconnectKeys(IVelvet velvet, HK akey, CK bkey) {
        if (akey == null || bkey == null) {
            throw new VelvetException("Attempt to connect a null key");
        }
        index(velvet).delete(akey, bkey);
    }

    @Override
    public boolean isConnectedKeys(IVelvet velvet, HK akey, CK bkey) {
        return index(velvet).contains(akey, bkey);
    }

}
