package com.zakgof.db.velvet.link;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet;

public abstract class AMultiGetter<HK, HV, CK, CV> implements IMultiGetter<HK, HV, CK, CV> {


    @Override
    public Map<HK, List<CV>> batchGet(IVelvet velvet, List<HV> nodes) {
        List<HK> hks = Stream.of(nodes).map(n -> getHostEntity().keyOf(n)).collect(Collectors.toList());
        Map<HK, List<CK>> keyMap = batchKeys(velvet, hks);
        List<CK> allCKs = Stream.of(keyMap.values()).flatMap(Stream::of).collect(Collectors.toList());
        Map<CK, CV> childMap = getChildEntity().batchGet(velvet, allCKs);
        Map<HK, List<CV>> result = Stream.of(keyMap.entrySet()).collect(Collectors.toMap(Entry::getKey, e -> Stream.of(e.getValue()).map(childMap::get).collect(Collectors.toList())));
        return result;
    }

    @Override
    public Map<HK, List<CK>> batchKeys(IVelvet velvet, List<HK> keys) {
        return Stream.of(keys).collect(Collectors.toMap(hk -> hk, hk -> keys(velvet, hk)));
    }

}
