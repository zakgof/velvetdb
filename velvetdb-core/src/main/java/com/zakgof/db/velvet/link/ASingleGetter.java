package com.zakgof.db.velvet.link;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
import com.zakgof.db.velvet.IVelvet;
import com.zakgof.tools.generic.Pair;

public abstract class ASingleGetter<HK, HV, CK, CV> implements ISingleGetter<HK, HV, CK, CV> {

    @Override
    public Map<HK, CV> batchGet(IVelvet velvet, List<HV> nodes) {
        List<HK> hks = Stream.of(nodes).map(n -> getHostEntity().keyOf(n)).collect(Collectors.toList());
        Map<HK, CK> keyMap = batchKeys(velvet, hks);
        List<CK> allCKs = new ArrayList<>(keyMap.values());
        Map<CK, CV> childMap = getChildEntity().batchGet(velvet, allCKs);
        Map<HK, CV> result = Stream.of(keyMap.entrySet()).collect(Collectors.toMap(Entry::getKey, e -> childMap.get(e.getValue())));
        return result;
    }

    @Override
    public Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
        return Stream.of(keys)
            .map(hk -> Pair.create(hk, key(velvet, hk)))
            .filter(p -> p.second() != null)
            .collect(Collectors.toMap(Pair::first, Pair::second));
    }

}
