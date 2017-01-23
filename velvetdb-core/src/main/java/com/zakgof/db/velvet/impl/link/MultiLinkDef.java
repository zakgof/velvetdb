package com.zakgof.db.velvet.impl.link;

import java.util.List;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.ILink;
import com.zakgof.db.velvet.IVelvet.LinkType;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.link.IMultiLinkDef;

public class MultiLinkDef<HK, HV, CK, CV> extends AVelvetLinkDef<HK, HV, CK, CV> implements IMultiLinkDef<HK, HV, CK, CV> {

    public MultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        super(hostEntity, childEntity, edgeKind);
    }

    public MultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        super(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> MultiLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity) {
        return new MultiLinkDef<>(hostEntity, childEntity);
    }

    public static <HK, HV, CK, CV> MultiLinkDef<HK, HV, CK, CV> of(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, String edgeKind) {
        return new MultiLinkDef<>(hostEntity, childEntity, edgeKind);
    }

    @Override
    public List<CV> multi(IVelvet velvet, HV node) {
        return getChildEntity().get(velvet, multiKeys(velvet, getHostEntity().keyOf(node)));
    }

    @Override
    public List<CK> multiKeys(IVelvet velvet, HK key) {
        return index(velvet, key).keys(getChildEntity().getKeyClass());
    }

    protected ILink<CK> index(IVelvet velvet, HK hkey) {
        return velvet.simpleIndex(hkey, getKind(), LinkType.Multi);
    }

    @Override
    public String toString() {
        return "multi " + super.toString();
    }

}
