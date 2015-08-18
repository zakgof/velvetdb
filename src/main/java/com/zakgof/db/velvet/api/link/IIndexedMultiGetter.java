package com.zakgof.db.velvet.api.link;

import com.zakgof.db.velvet.api.query.IIndexQuery;

public interface IIndexedMultiGetter<HK, HV, CK, CV, M extends Comparable<M>> {
  IMultiGetter<HK, HV, CK, CV> indexed(IIndexQuery<M> indexQuery);
}
