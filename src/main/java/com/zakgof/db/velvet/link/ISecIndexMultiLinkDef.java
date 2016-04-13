package com.zakgof.db.velvet.link;

import java.util.function.Function;

public interface ISecIndexMultiLinkDef <HK, HV, CK, CV, M extends Comparable<? super M>> extends IIndexedMultiLink<HK, HV, CK, CV, M> {
	public Function<CV, M> getMetric();
}
