package com.zakgof.db.velvet.link;

import java.util.function.Function;

public interface ISecSortedMultiLinkDef<HK, HV, CK, CV, M extends Comparable<? super M>> extends ISortedMultiLink<HK, HV, CK, CV, M> {
    public Function<CV, M> getMetric();
}
