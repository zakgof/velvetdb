package com.zakgof.db.velvet.link;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;

public interface ISingleGetter<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {

    public CV get(IVelvet velvet, HV node);

    public CK key(IVelvet velvet, HK key);

    public default Map<HK, CV> batchGet(IVelvet velvet, List<HV> nodes) {


    }

    public default Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
        return keys.stream().collect(Collectors.toMap(hk -> hk, hk -> key(velvet, hk)));
    }

}
