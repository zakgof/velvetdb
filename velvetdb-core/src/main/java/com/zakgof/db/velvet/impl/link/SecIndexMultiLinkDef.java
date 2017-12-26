package com.zakgof.db.velvet.impl.link;

import java.util.function.Function;

import com.zakgof.db.velvet.IVelvet;
import com.zakgof.db.velvet.IVelvet.IKeyIndexLink;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.impl.entity.EntityDef;
import com.zakgof.db.velvet.link.ISecSortedMultiLinkDef;

public class SecIndexMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends AIndexMultiLinkDef<HK, HV, CK, CV, M> implements ISecSortedMultiLinkDef<HK, HV, CK, CV, M> {

    private final Function<CV, M> metric;
    private Class<M> mclazz;

    public SecIndexMultiLinkDef(IEntityDef<HK, HV> hostEntity, IEntityDef<CK, CV> childEntity, Class<M> mclazz, Function<CV, M> metric) {
        super(hostEntity, childEntity);
        this.metric = metric;
        this.mclazz = mclazz;
    }

    @Override
    protected IKeyIndexLink<HK, CK, M> index(IVelvet velvet, HK akey) {
        return velvet.<HK, CK, CV, M> secondaryKeyIndex(akey, getHostEntity().getKeyClass(), getKind(), metric, mclazz, getChildEntity().getKeyClass(), ((EntityDef<CK, CV>) getChildEntity()).store(velvet));
    }

    @Override
    public Function<CV, M> getMetric() {
        return metric;
    }
}
