package com.zakgof.db.velvet.link;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.tools.generic.Pair;

public interface ISingleGetter<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {

    public CV get(IVelvet velvet, HV node);

    public CK key(IVelvet velvet, HK key);

    public default Map<HK, CV> batchGet(IVelvet velvet, List<HV> nodes) {
        List<HK> hks = nodes.stream().map(n -> getHostEntity().keyOf(n)).collect(Collectors.toList());
        Map<HK, CK> keyMap = batchKeys(velvet, hks);
        List<CK> allCKs = new ArrayList<>(keyMap.values());
        Map<CK, CV> childMap = getChildEntity().batchGetMap(velvet, allCKs);
        Map<HK, CV> result = Maps.transformValues(keyMap, childMap::get);
        return result;
    }

    public default Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
        return keys.stream()
            .map(hk -> Pair.create(hk, key(velvet, hk)))
            .filter(p -> p.second() != null)
            .collect(Collectors.toMap(Pair::first, Pair::second));
    }

}
