package com.zakgof.db.velvet.impl.link;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.annimon.stream.Collectors;
import com.annimon.stream.Stream;
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
    public Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
        return index(velvet).batchGet(keys);
    }

    @Override
    ISingleLink<HK, CK> index(IVelvet velvet) {
        return velvet.singleLink(getHostEntity().getKeyClass(), getChildEntity().getKeyClass(), getKind());
    }

    @Override
    public String toString() {
        return "single " + super.toString();
    }

    @Override
    public Map<HK, CV> batchGet(IVelvet velvet, List<HV> nodes) {
        List<HK> hks = Stream.of(nodes).map(n -> getHostEntity().keyOf(n)).collect(Collectors.toList());
        Map<HK, CK> keyMap = batchKeys(velvet, hks);
        List<CK> allCKs = new ArrayList<>(keyMap.values());
        Map<CK, CV> childMap = getChildEntity().batchGetMap(velvet, allCKs);
        Map<HK, CV> result = Stream.of(keyMap.entrySet()).collect(Collectors.toMap(Entry::getKey, e -> childMap.get(e.getValue())));
        return result;
    }

//    @Override
//    public Map<HK, CK> batchKeys(IVelvet velvet, List<HK> keys) {
//        return Stream.of(keys)
//            .map(hk -> Pair.create(hk, key(velvet, hk)))
//            .filter(p -> p.second() != null)
//            .collect(Collectors.toMap(Pair::first, Pair::second));
//    }

}
