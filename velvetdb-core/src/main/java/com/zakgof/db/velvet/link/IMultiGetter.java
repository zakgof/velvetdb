package com.zakgof.db.velvet.link;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.zakgof.db.velvet.IVelvet;

public interface IMultiGetter<HK, HV, CK, CV> extends IRelation<HK, HV, CK, CV> {

    public List<CV> get(IVelvet velvet, HV node);

    public List<CK> keys(IVelvet velvet, HK key);

    public default Map<HK, List<CV>> batchGet(IVelvet velvet, List<HV> nodes) {
        List<HK> hks = nodes.stream().map(n -> getHostEntity().keyOf(n)).collect(Collectors.toList());
        Map<HK, List<CK>> keyMap = batchKeys(velvet, hks);
        List<CK> allCKs = keyMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
        Map<CK, CV> childMap = getChildEntity().batchGet(velvet, allCKs);
        Map<HK, List<CV>> result = keyMap.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream().map(childMap::get).collect(Collectors.toList())));
        return result;
    }

    public default Map<HK, List<CK>> batchKeys(IVelvet velvet, List<HK> keys) {
        return keys.stream().collect(Collectors.toMap(hk -> hk, hk -> keys(velvet, hk)));
    }

}
