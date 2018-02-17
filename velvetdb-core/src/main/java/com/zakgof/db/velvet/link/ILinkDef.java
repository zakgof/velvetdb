package com.zakgof.db.velvet.link;

import com.zakgof.db.velvet.IVelvet;

public interface ILinkDef<HK, HV, CK, CV> extends IReadOnlyLinkDef<HK, HV, CK, CV> {

    public void connectKeys(IVelvet velvet, HK akey, CK bkey);

    // public void batchConnectKeys(IVelvet velvet, Pair<HK, CK> connections);

    public void disconnectKeys(IVelvet velvet, HK akey, CK bkey);

    public default void connect(IVelvet velvet, HV a, CV b) {
        connectKeys(velvet, getHostEntity().keyOf(a), getChildEntity().keyOf(b));
    }

    public default void disconnect(IVelvet velvet, HV a, CV b) {
        disconnectKeys(velvet, getHostEntity().keyOf(a), getChildEntity().keyOf(b));
    }
}
