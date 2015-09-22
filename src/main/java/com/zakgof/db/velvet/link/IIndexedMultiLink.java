package com.zakgof.db.velvet.link;

public interface IIndexedMultiLink<HK, HV, CK, CV, M extends Comparable<M>> extends IMultiLinkDef<HK, HV, CK, CV>, IIndexedMultiGetter<HK, HV, CK, CV, M> {
}