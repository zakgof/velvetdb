package com.zakgof.db.velvet.link;

import java.util.function.Function;

public interface ISecMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends IMultiLinkDef<HK, HV, CK, CV>, ISecMultiGetter<HK, HV, CK, CV, M> {

    public Function<CV, M> getMetric();

}
