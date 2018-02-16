package com.zakgof.db.velvet.link;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;

public interface ISingleGetter<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {

    public CV get(IVelvet velvet, HV node);

    public CK key(IVelvet velvet, HK key);

    public default Map<HK, CV> batchGet(IVelvet velvet, List<HV> nodes) {
        List<HK> hks = nodes.stream().map(n -> getHostEntity().keyOf(n)).collect(Collectors.toList());
        Map<HK, CK> keyMap = batchKeys(velvet, hks);
        List<CK> allCKs = new ArrayList<>(keyMap.values());
        Map<CK, CV> childMap = getChildEntity().batchGet(velvet, allCKs);
        Map<HK, CV> result = keyMap.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> childMap.get(e.getValue())));
        return result;
    }

    public default Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
        return keys.stream().collect(Collectors.toMap(hk -> hk, hk -> key(velvet, hk)));
    }

}
